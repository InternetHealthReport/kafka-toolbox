import argparse
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from datetime import datetime, timezone
import logging
import msgpack
import requests
import sys


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
    """Fetch ix/ixlan/ixpfx data from PeeringDB and push the merged
    entries to the Kafka topic.

    Push one entry per ixpfx to the topic. The entry also contains
    information about the corresponding ix (id, name, name_long, and
    country) as well as the ixlan_id it belongs to.
    The ix_id is used as the key and the 'updated' field (of the ixpfx
    entry) as the timestamp.
    """
    # Getting the ix_id from the ixpfx requires an additional hop over
    # the ixlan since there is no direct connection.
    # Get ix data.
    ix_data = query_pdb('ix')
    if len(ix_data) == 0:
        return
    ix_data_dict = dict()
    for entry in ix_data['data']:
        if entry['id'] in ix_data_dict:
            logging.warning('Duplicate ix id: {}. Ignoring entry {}'
                            .format(entry['id'], entry))
            continue
        ix_data_dict[entry['id']] = entry
    # Get ixlan data.
    ixlan_data = query_pdb('ixlan')
    if len(ixlan_data) == 0:
        return
    # Construct a map ixlan_id -> ix_id.
    ixlan_ix_map = dict()
    for entry in ixlan_data['data']:
        if entry['id'] in ixlan_ix_map:
            logging.warning('Duplicate ixlan id: {}. Ignoring entry {}.'
                            .format(entry['id'], entry))
            continue
        ixlan_ix_map[entry['id']] = entry['ix_id']
    # Get ixpfx data.
    ixpfx_data = query_pdb('ixpfx')
    if len(ixpfx_data) == 0:
        return
    for entry in ixpfx_data['data']:
        proto = entry['protocol']
        if not (proto == 'IPv4' or proto == 'IPv6'):
            logging.warning('Unknown protocol specified for ixpfx {}: {}'
                            .format(entry['id'], proto))
            continue
        ixlan_id = entry['ixlan_id']
        if ixlan_id not in ixlan_ix_map:
            logging.warning('Failed to find ixlan {} for ixpfx {}.'
                            .format(ixlan_id, entry['id']))
            continue
        ix_id = ixlan_ix_map[ixlan_id]
        if ix_id not in ix_data_dict:
            logging.warning('Failed to find ix {} for ixlan {} / ixpfx {}.'
                            .format(ix_id, ixlan_id, entry['id']))
            continue
        ix_info = ix_data_dict[ix_id]
        key = ix_id
        value = {'ix_id': ix_id,
                 'name': ix_info['name'],
                 'name_long': ix_info['name_long'],
                 'country': ix_info['country'],
                 'ixlan_id': ixlan_id,
                 'ixpfx_id': entry['id'],
                 'protocol': proto,
                 'prefix': entry['prefix']}
        timestamp = transform_iso8601_to_epoch(entry['updated'])
        if timestamp == 0:
            logging.warning('Ignored ixpfx entry due to bad timestamp: {}'
                            .format(entry))
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


if __name__ == '__main__':
    desc = """This script queries ix/ixlan/ixpfx data from PeeringDB and pushes
    it into a Kafka topic. ixpfx data can be used to map an IP prefix to the
    corresponding IXP."""
    parser = argparse.ArgumentParser(description=desc)
    args = parser.parse_args()

    # Logging
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT, filename='ihr-kafka-ix.log',
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S'
        )
    logging.info("Started: %s" % sys.argv)
    logging.info("Arguments: %s" % args)

    topic = 'ihr_peeringdb_ix'
    prepare_topic(topic)
    producer = Producer({'bootstrap.servers':
                             'kafka1:9092,kafka2:9092,kafka3:9092',
                         # 'linger.ms': 1000,
                         'default.topic.config':
                             {'compression.codec': 'snappy'}})
    fetch_and_produce_data(producer, topic)
