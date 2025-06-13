import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import bgpkit
import msgpack
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

DATE_FMT = '%Y-%m-%dT%H:%M:%S'
ELEMENTS_PER_RECORD = 20


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
       Triggered by poll() or flush().
    """
    if err is not None:
        logging.error(f'Message delivery failed: {err}')


def getElementDict(element: dict):
    elementDict = dict()
    elementDict['type'] = element.elem_type
    elementDict['time'] = element.timestamp
    elementDict['peer_asn'] = element.peer_asn
    elementDict['peer_address'] = element.peer_ip
    elementDict['fields'] = {
        'next-hop': element.next_hop,
        'as-path': element.as_path,
        'communities': list() if not element.communities else element.communities,
        'prefix': element.prefix
    }
    return elementDict


def produceKafkaMessages(producer: Producer, topic: str, record: dict):
    recordTimestamp = int(record['rec']['time'] * 1000)
    try:
        producer.produce(
            topic,
            msgpack.packb(record, use_bin_type=True),
            callback=delivery_report,
            timestamp=recordTimestamp
        )

        # Trigger any available delivery report callbacks from previous produce() calls
        producer.poll(0)
    except BufferError:
        logging.warning('Buffer error, the queue must be full! Flushing...')
        producer.flush()

        logging.info('Queue flushed, will write the message again')
        producer.produce(
            topic,
            msgpack.packb(record, use_bin_type=True),
            callback=delivery_report,
            timestamp=recordTimestamp
        )
        producer.poll(0)


def pushData(record_type: str, collector: str, startts: str, endts: str):
    bgpkit_record_type = record_type
    if bgpkit_record_type == 'ribs':
        # BGPKIT uses "rib" instead of "ribs".
        bgpkit_record_type = 'rib'
    broker = bgpkit.Broker()
    items = broker.query(
        ts_start=startts,
        ts_end=endts,
        collector_id=collector,
        data_type=bgpkit_record_type
    )
    data_items = list(items)
    if not data_items:
        logging.error('No data items available.')
        return

    # Create kafka topic
    topic = 'ihr_bgp_' + collector + '_' + record_type
    admin_client = AdminClient({'bootstrap.servers': KAFKA_HOST})

    topic_list = [NewTopic(topic, num_partitions=1, replication_factor=2)]
    created_topic = admin_client.create_topics(topic_list)
    for topic, f in created_topic.items():
        try:
            f.result()  # The result itself is None
            logging.info(f'Topic {topic} created')
        except Exception as e:
            logging.warning(f'Failed to create topic {topic}: {e}')

    # Create producer
    producer = Producer({'bootstrap.servers': KAFKA_HOST,
                         'queue.buffering.max.messages': 10000000,
                         'queue.buffering.max.kbytes': 2097151,
                         'linger.ms': 200,
                         'batch.num.messages': 1000000,
                         'message.max.bytes': 999000,
                         'default.topic.config': {'compression.codec': 'snappy'}})

    # BGPKIT flattens records and only provides elements, so we have to make our own
    # records. Technically none of these fields are accessed by other code, but we keep
    # at least fields with useful information.
    # The number of elements per record define the message size in Kafka.
    recordDict = dict()
    if 'rrc' in collector:
        recordDict['project'] = 'ris'
    else:
        recordDict['project'] = 'routeviews'
    recordDict['collector'] = collector
    if record_type == 'updates':
        recordDict['type'] = 'update'
    else:
        recordDict['type'] = 'rib'
    recordDict['dump_time'] = 0
    recordDict['time'] = 0

    currRecord = recordDict.copy()
    currElements = list()
    for item in items:
        logging.info(f'Processing file: {item.url} Size: {item.rough_size / 1024 / 1024:.2f}MiB')
        parser = bgpkit.Parser(item.url)
        for elem in parser:
            elementDict = getElementDict(elem)
            if (currRecord['time'] != 0
                and elementDict['time'] != currRecord['time']
                    and currElements):
                # For updates, push at least one message per unique timestamp,
                # independent of record size.
                produceKafkaMessages(producer, topic, {'rec': currRecord, 'elements': currElements})
                currRecord = recordDict.copy()
                currElements = list()
            if record_type == 'ribs':
                # BGPKIT uses 'A' as type in RIBs.
                elementDict['type'] = 'R'
            if currRecord['time'] == 0:
                currRecord['time'] = elementDict['time']
                currRecord['dump_time'] = elementDict['time']
            currElements.append(elementDict)
            if len(currElements) == ELEMENTS_PER_RECORD:
                produceKafkaMessages(producer, topic, {'rec': currRecord, 'elements': currElements})
                currRecord = recordDict.copy()
                currElements = list()
    # Push trailing elements.
    if currElements:
        produceKafkaMessages(producer, topic, {'rec': currRecord, 'elements': currElements})

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    producer.flush()


if __name__ == '__main__':

    global KAFKA_HOST
    KAFKA_HOST = os.environ['KAFKA_HOST']

    text = """This script pushes BGP data from specified collector for the specified
    time window to a Kafka topic. The created topic has only one partition in order to
    make sequential reads easy. If no start and end time is given then it downloads data
    for the current hour."""

    parser = argparse.ArgumentParser(description=text)
    parser.add_argument('--collector', '-c', help='Choose collector to push data for')
    parser.add_argument('--startTime', '-s',
                        help='Choose start time (Format: Y-m-dTH:M:S; Example: 2017-11-06T16:00:00)')
    parser.add_argument('--endTime', '-e', help='Choose end time (Format: Y-m-dTH:M:S; Example: 2017-11-06T16:00:00)')
    parser.add_argument('--type', '-t', help='Choose record type: ribs or updates')

    args = parser.parse_args()

    # initialize recordType
    if args.type:
        if args.type in ['ribs', 'updates']:
            recordType = args.type
        else:
            sys.exit('Incorrect type specified; Choose from ribs or update')
    else:
        sys.exit('Record type not specified')

    # initialize collector
    if args.collector:
        collector = args.collector
    else:
        sys.exit('Collector not specified')

    # Default timing
    #   RIBs
    #     RouteViews produces RIBs every two hours, RIS every 8 hours.
    #     info: Default timing might break if this script is called while the RIB is
    #     being produced. For example, calling this script for RouteViews at 00:01 will
    #     ignore the RIB at 22:00, but the RIB at 00:00 is potentially not yet
    #     available.
    #   Updates
    #     Updates are produced every 15 minutes. First, round down the current time to
    #     the last 15 minute increment. Let this be time t, the fetch data from t - 30
    #     minutes to t - 15 minutes. This should ensure availability of the file.

    # initialize time to start
    timeWindowInMinutes = 15
    currentTime = datetime.now(tz=timezone.utc)
    minuteStart = int(currentTime.minute / timeWindowInMinutes) * timeWindowInMinutes
    if args.startTime:
        try:
            timeStart = datetime.strptime(args.startTime, DATE_FMT).replace(tzinfo=timezone.utc)
        except ValueError as e:
            sys.exit(f'Invalid start time specified: {e}')
    else:
        if recordType == 'updates':
            # Update files are produced very 15 minutes, but take some time to be
            # created.
            timeStart = currentTime.replace(microsecond=0, second=0, minute=minuteStart) - \
                timedelta(minutes=2 * timeWindowInMinutes)
        else:
            # RouteViews produces RIBs every 2 hours, RIS every 8 hours.
            delayInMinutes = 120
            if 'rrc' in collector:
                delayInMinutes = 480
            timeStart = currentTime-timedelta(minutes=delayInMinutes)

    # initialize time to end
    if args.endTime:
        try:
            timeEnd = datetime.strptime(args.endTime, DATE_FMT).replace(tzinfo=timezone.utc)
        except ValueError as e:
            sys.exit(f'Invalid end time specified: {e}')
    else:
        if recordType == 'updates':
            timeEnd = currentTime.replace(microsecond=0, second=0, minute=minuteStart) - \
                timedelta(minutes=1 * timeWindowInMinutes)
        else:
            timeEnd = currentTime

    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[logging.StreamHandler()]
    )
    logging.info(f'Started: {sys.argv}')
    logging.info(f'start time: {timeStart}, end time: {timeEnd}')

    logging.info(f'Downloading {recordType} data for {collector}')
    pushData(recordType, collector, timeStart.strftime(DATE_FMT), timeEnd.strftime(DATE_FMT))

    logging.info(f'End: {sys.argv}')
