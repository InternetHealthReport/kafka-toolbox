import datetime
import calendar
import json
import os
import msgpack
import logging
import requests
import sys
import configparser
import argparse
from datetime import timedelta
from requests_futures.sessions import FuturesSession
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from confluent_kafka import Producer, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic


def valid_date(s):
    """Parse date from config file"""

    try:
        return datetime.datetime.strptime(s+"UTC", "%Y-%m-%dT%H:%M%Z")
    except ValueError:
        # Not a valid date:
        return None


def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
    max_workers=8,
):
    """ Retry if there is a problem"""

    session = session or FuturesSession(max_workers=max_workers)
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def worker_task(resp, *args, **kwargs):
    """Process json in background"""

    try:
        resp.data = resp.json()
    except json.decoder.JSONDecodeError:
        logging.error("Error while reading Atlas json data.\n")
        resp.data = {}


def cousteau_on_steroid(params, retry=3):
    """Query the REST API in parallel"""

    url = "https://atlas.ripe.net/api/v2/measurements/{0}/results"
    req_param = {
            "start": int(calendar.timegm(params["start"].timetuple())),
            "stop": int(calendar.timegm(params["stop"].timetuple())),
            }

    if params["probe_ids"]:
        req_param["probe_ids"] = params["probe_ids"]

    queries = []

    session = requests_retry_session()
    for msm in params["msm_id"]:
        queries.append([session.get(
                                    url=url.format(msm), params=req_param,
                                    hooks={'response': worker_task}
                                    ),
                       [url.format(msm), req_param]])

    for tmp in queries:
        query = tmp[0]
        try:
            resp = query.result()

            yield (resp.ok, resp.data)
        except requests.exceptions.ChunkedEncodingError:
            logging.error("Could not retrieve traceroutes for {}".format(query))
            logging.error(tmp[1])


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        # print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        pass


def fetch_measurement_ids(topic):
    """
    Fetches Atlas measurement ids from kafka topic: atlas_measurements
    """
    logging.info("Start consuming measurement ids from Kafka..")

    consumer = Consumer({
        'bootstrap.servers': KAFKA_HOST,
        'group.id': 'measurement_ids',
        'enable.auto.commit': False,
    })

    partition = TopicPartition(topic, 0)
    low, high = consumer.get_watermark_offsets(partition)
    offset = high - 1

    if offset < 0:
        logging.info("Didn't find measurement ids. Existing..")
        sys.exit('No Measurement ids found.')
    else:
        partition = TopicPartition(topic, 0, offset)

    consumer.assign([partition])

    msg = consumer.poll(10)
    ids = msgpack.unpackb(msg.value(), raw=False)
    consumer.close()

    logging.info("Got {} measuremnet ids".format(len(ids)))

    logging.info("Finished consuming measurement ids from Kafka")
    return ids


if __name__ == '__main__':
    KAFKA_HOST = os.environ["KAFKA_HOST"]

    # Command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "-C", "--config_file",
            help="Get all parameters from the specified config file",
            type=str, default="conf/ihr-default.conf")
    args = parser.parse_args()

    # Logging
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format='%(asctime)s %(processName)s %(message)s',
            level=logging.INFO,
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[logging.StreamHandler()]
            )
    logging.info("Started: %s" % sys.argv)
    logging.info("Arguments: %s" % args)

    # Read the config file
    config = configparser.ConfigParser()
    config.read(args.config_file)

    # atlas_msm_ids =  [int(x) for x in config.get("io", "msm_ids").split(",") if x]
    logging.info("Start consumption")
    msm_ids_topic = "atlas_measurement_ids"
    atlas_msm_ids = fetch_measurement_ids(msm_ids_topic)
    atlas_probe_ids = [int(x) for x in config.get("io", "probe_ids").split(",") if x]
    logging.info("End consumption")

    atlas_start = valid_date(config.get("io", "start"))
    atlas_stop = valid_date(config.get("io", "stop"))

    # No data given:
    # Fetch the last 5 min of data that happened -10 to -5 min ago
    if atlas_start is None or atlas_stop is None:
        currentTime = datetime.datetime.now(datetime.timezone.utc)
        atlas_start = currentTime.replace(microsecond=0, second=0)-timedelta(minutes=10)
        atlas_stop = currentTime.replace(microsecond=0, second=0)-timedelta(minutes=5)
        logging.warning('start and end times: {}, {}'.format(atlas_start, atlas_stop))

    chunk_size = int(config.get('io', 'chunk_size'))

    # Create kafka topic
    topic = config.get("io", "kafka_topic")
    admin_client = AdminClient({'bootstrap.servers': KAFKA_HOST})

    topic_list = [NewTopic(topic, num_partitions=3, replication_factor=2)]
    created_topic = admin_client.create_topics(topic_list)
    for topic, f in created_topic.items():
        try:
            f.result()  # The result itself is None
            logging.warning("Topic {} created".format(topic))
        except Exception as e:
            logging.warning("Failed to create topic {}: {}".format(topic, e))

    # Create producer
    producer = Producer({'bootstrap.servers': KAFKA_HOST,
                         'queue.buffering.max.messages': 10000000,
                         'queue.buffering.max.kbytes': 2097151,
                         'linger.ms': 200,
                         'batch.num.messages': 1000000,
                         'message.max.bytes': 999000,
                         'default.topic.config': {'compression.codec': 'snappy'}})

    # Fetch data from RIPE
    current_time = atlas_start
    end_time = atlas_stop
    end_epoch = int(calendar.timegm(end_time.timetuple()))
    while current_time < end_time:
        logging.warning("downloading: "+str(current_time))
        params = {"msm_id": atlas_msm_ids, "start": current_time,
                  "stop": current_time + timedelta(seconds=chunk_size),
                  "probe_ids": atlas_probe_ids}

        for is_success, data in cousteau_on_steroid(params):
            if is_success:
                for traceroute in data:

                    if traceroute['timestamp'] >= end_epoch:
                        continue
                    try:
                        logging.debug('going to produce something')
                        producer.produce(
                                topic,
                                msgpack.packb(traceroute, use_bin_type=True),
                                traceroute['msm_id'].to_bytes(8, byteorder='big'),
                                callback=delivery_report,
                                timestamp=traceroute.get('timestamp')*1000
                                )

                        logging.debug('produced something')
                        # Trigger any available delivery report callbacks from previous produce() calls
                        producer.poll(0)

                    except KeyError:
                        logging.error('Ignoring one traceroute: {}'.format(traceroute))
                    except BufferError:
                        logging.error('Local queue is full ')
                        producer.flush()
                        producer.produce(
                                topic,
                                msgpack.packb(traceroute, use_bin_type=True),
                                traceroute['msm_id'].to_bytes(8, byteorder='big'),
                                callback=delivery_report,
                                timestamp=traceroute.get('timestamp')*1000
                                )

                        # Trigger any available delivery report callbacks from previous produce() calls
                        producer.poll(0)

            else:
                logging.error("Error could not load the data")

            # Wait for any outstanding messages to be delivered and delivery report
            # callbacks to be triggered.
            producer.flush()

        current_time = current_time + timedelta(seconds=chunk_size)
