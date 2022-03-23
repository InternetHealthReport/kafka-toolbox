import argparse
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from datetime import datetime, timezone
import logging
import msgpack
import requests
import sys


def get_message_dict(data: dict) -> dict:
    """Copy only relevant fields, rename fields where necessary, and
    return the reduced dictionary.
    """
    ret = dict()
    ret['ix_id'] = data['ix_id']
    ret['name'] = data['name']
    ret['netixlan_id'] = data['id']
    ret['asn'] = data['asn']
    ret['ipaddr4'] = data['ipaddr4']
    ret['ipaddr6'] = data['ipaddr6']
    return ret


def transform_iso8601_to_epoch(timestamp: str) -> int:
    """Return the given timestamp as a UNIX epoch (in seconds).
    Timestamp is expected to be in UTC and obey the format
    %Y-%m-%dT%H:%M:%SZ.
    """
    # Python's datetime.fromisoformat() does not support the timezone
    # specification used by PeeringDB.
    fmt_string = '%Y-%m-%dT%H:%M:%SZ'
    try:
        # Treat timestamp as UTC
        epoch = datetime.strptime(timestamp, fmt_string) \
            .replace(tzinfo=timezone.utc).timestamp()
    except ValueError as e:
        logging.error('Failed to parse timestamp {}. Error: {}'
                      .format(timestamp, e))
        return 0
    return int(epoch)


def query_pdb(endpoint: str, params=None) -> dict:
    """Query the PeeringDB API with the specified endpoint and
    parameters and return the response JSON data as a dictionary.
    """
    peeringdb_api_base = 'https://peeringdb.com/api/'
    if params is None:
        params = dict()
    url = peeringdb_api_base + endpoint
    # Always query only 'ok' entries
    params['status'] = 'ok'
    logging.info('Querying PeeringDB {} with params {}'.format(url, params))
    try:
        r = requests.get(url, params)
    except ConnectionError as e:
        logging.error('Failed to connect to PeeringDB: {}'.format(e))
        return dict()
    if r.status_code != 200:
        logging.error('PeeringDB replied with status code: {}'
                      .format(r.status_code))
        return dict()
    try:
        json_data = r.json()
    except ValueError as e:
        logging.error('Failed to decode JSON reply: {}'.format(e))
        return dict()
    return json_data


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery
    result.
    Triggered by poll() or flush().
    """
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        # print('Message delivered to {} [{}]'
        # .format(msg.topic(), msg.partition()))
        pass


def prepare_topic(topic: str) -> None:
    """Try to create the specified topic on the Kafka servers.
    Output a warning if the topic already exists.
    """
    admin_client = AdminClient({'bootstrap.servers':
                                    'kafka1:9092, kafka2:9092, kafka3:9092'})
    topic_list = [NewTopic(topic, num_partitions=3, replication_factor=2)]
    created_topic = admin_client.create_topics(topic_list)
    for topic, f in created_topic.items():
        try:
            f.result()  # The result itself is None
            logging.warning("Topic {} created".format(topic))
        except Exception as e:
            logging.warning("Failed to create topic {}: {}".format(topic, e))


def fetch_and_produce_data(producer: Producer, topic: str) -> None:
    """Fetch netixlan data from PeeringDB and push the (reduced) entries
    to the Kafka topic.

    The ix_id is used as the key and the 'updated' field as the
    timestamp. Only fields that are specified in the get_message_dict
    method are pushed to Kafka.
    """
    json_data = query_pdb('netixlan')
    if len(json_data) == 0:
        return
    for entry in json_data['data']:
        key = entry['ix_id']
        value = get_message_dict(entry)
        timestamp = transform_iso8601_to_epoch(entry['updated'])
        if timestamp == 0:
            logging.warning('Ignored entry due to bad timestamp: {}'
                            .format(value))
            continue
        producer.produce(topic,
                         msgpack.packb(value, use_bin_type=True),
                         key.to_bytes(8, byteorder='big'),
                         callback=delivery_report,
                         timestamp=timestamp * 1000)
        # Trigger any available delivery report callbacks from previous
        # produce() calls
        producer.poll(0)
    # Wait for any outstanding messages to be delivered and delivery
    # report callbacks to be triggered.
    producer.flush()
    return


if __name__ == '__main__':
    desc = """This script queries netixlan data from PeeringDB and pushes it
    into a Kafka topic. netixlan data provides information about the routers
    that participate in an IXP and enables a mapping of IPs that are within the
    IXP prefix to the ASN of the corresponding border routers."""
    parser = argparse.ArgumentParser(description=desc)
    args = parser.parse_args()

    # Logging
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT, filename='ihr-kafka-netixlan.log',
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S'
        )
    logging.info("Started: %s" % sys.argv)
    logging.info("Arguments: %s" % args)

    topic = 'ihr_peeringdb_netixlan'
    prepare_topic(topic)
    producer = Producer({'bootstrap.servers':
                             'kafka1:9092,kafka2:9092,kafka3:9092',
                         # 'linger.ms': 1000,
                         'default.topic.config':
                             {'compression.codec': 'snappy'}})
    fetch_and_produce_data(producer, topic)
