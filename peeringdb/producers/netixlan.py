import argparse
import os
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
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
    logging.info(f'Querying PeeringDB {url} with params {params}')
    try:
        r = requests.get(url, params)
    except ConnectionError as e:
        logging.error(f'Failed to connect to PeeringDB: {e}')
        return dict()
    if r.status_code != 200:
        logging.error(f'PeeringDB replied with status code: {r.status_code}')
        return dict()
    try:
        json_data = r.json()
    except ValueError as e:
        logging.error(f'Failed to decode JSON reply: {e}')
        return dict()
    return json_data


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery
    result.

    Triggered by poll() or flush().
    """
    if err is not None:
        logging.error(f'Message delivery failed: {err}')
    else:
        # print('Message delivered to {} [{}]'
        # .format(msg.topic(), msg.partition()))
        pass


def prepare_topic(topic: str) -> None:
    """Try to create the specified topic on the Kafka servers.

    Output a warning if the topic already exists.
    """
    admin_client = AdminClient({'bootstrap.servers': KAFKA_HOST})
    topic_list = [NewTopic(topic, num_partitions=3, replication_factor=2)]
    created_topic = admin_client.create_topics(topic_list)
    for topic, f in created_topic.items():
        try:
            f.result()  # The result itself is None
            logging.warning(f'Topic {topic} created')
        except Exception as e:
            logging.warning(f'Failed to create topic {topic}: {e}')


def fetch_and_produce_data(producer: Producer, topic: str) -> None:
    """Fetch netixlan data from PeeringDB and push the (reduced)
    entries to the Kafka topic.

    The ix_id is used as the key. Only fields that are specified in
    the get_message_dict method are pushed to Kafka.
    """
    json_data = query_pdb('netixlan')
    if len(json_data) == 0:
        return
    for entry in json_data['data']:
        key = entry['ix_id']
        value = get_message_dict(entry)
        producer.produce(topic,
                         msgpack.packb(value, use_bin_type=True),
                         key.to_bytes(8, byteorder='big'),
                         callback=delivery_report)
        # Trigger any available delivery report callbacks from previous
        # produce() calls
        producer.poll(0)
    # Wait for any outstanding messages to be delivered and delivery
    # report callbacks to be triggered.
    producer.flush()
    return


def main() -> None:
    desc = """This script queries netixlan data from PeeringDB and pushes it
    into a Kafka topic. netixlan data provides information about the routers
    that participate in an IXP and enables a mapping of IPs that are within the
    IXP prefix to the ASN of the corresponding border routers."""
    parser = argparse.ArgumentParser(description=desc)
    args = parser.parse_args()

    # Logging
    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(format=FORMAT,
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        handlers=[logging.StreamHandler()]
                        )
    logging.info(f'Started: {sys.argv}')

    topic = 'ihr_peeringdb_netixlan'
    prepare_topic(topic)
    producer = Producer({'bootstrap.servers': KAFKA_HOST,
                         'default.topic.config': {'compression.codec': 'snappy'}})
    fetch_and_produce_data(producer, topic)


if __name__ == '__main__':
    global KAFKA_HOST
    KAFKA_HOST = os.environ["KAFKA_HOST"]

    main()
    sys.exit(0)
