import sys
import argparse
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pybgpstream import BGPStream
from datetime import datetime
from datetime import timedelta
import msgpack
import logging


def dt2ts(dt):
    return int((dt - datetime(1970, 1, 1)).total_seconds())

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        # print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        pass


def getRecordDict(record):
    recordDict = {}

    recordDict["project"] = record.project
    recordDict["collector"] = record.collector
    recordDict["type"] = record.type
    recordDict["dump_time"] = record.dump_time
    recordDict["time"] = record.time
    recordDict["status"] = record.status
    recordDict["dump_position"] = record.dump_position

    return recordDict


def getElementDict(element):
    elementDict = {}

    elementDict["type"] = element.type
    elementDict["time"] = element.time
    elementDict["peer_asn"] = element.peer_asn
    elementDict["peer_address"] = element.peer_address
    elementDict["fields"] = element.fields
    if 'communities' in element.fields:
        elementDict['fields']['communities'] = list(element.fields['communities'])

    return elementDict


def pushData(record_type, collector, startts, endts):

    stream = BGPStream(
            from_time=str(startts), until_time=str(endts), collectors=[collector],
            record_type=record_type
            )

    # Create kafka topic
    topic = "ihr_bgp_" + collector + "_" + record_type
    admin_client = AdminClient({'bootstrap.servers':'kafka1:9092, kafka2:9092, kafka3:9092'})

    topic_list = [NewTopic(topic, num_partitions=1, replication_factor=2)]
    created_topic = admin_client.create_topics(topic_list)
    for topic, f in created_topic.items():
        try:
            f.result()  # The result itself is None
            logging.warning("Topic {} created".format(topic))
        except Exception as e:
            logging.warning("Failed to create topic {}: {}".format(topic, e))

    # Create producer
    producer = Producer({'bootstrap.servers': 'kafka1:9092,kafka2:9092,kafka3:9092',
        # 'linger.ms': 1000, 
        'default.topic.config': {'compression.codec': 'snappy'}}) 
    
    for rec in stream.records():
        completeRecord = {}
        completeRecord["rec"] = getRecordDict(rec)
        completeRecord["elements"] = []

        recordTimeStamp = int(rec.time*1000)

        for elem in rec:
            elementDict = getElementDict(elem)
            completeRecord["elements"].append(elementDict)

        producer.produce(
                topic, 
                msgpack.packb(completeRecord, use_bin_type=True), 
                callback=delivery_report,
                timestamp = recordTimeStamp
                )

        # Trigger any available delivery report callbacks from previous produce() calls
        producer.poll(0)

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    producer.flush()


if __name__ == '__main__':

    text = "This script pushes BGP data from specified collector(s) \
for the specified time window to Kafka topic(s). The created topics have only \
one partition in order to make sequential reads easy. If no start and end time \
is given then it download data for the current hour."

    parser = argparse.ArgumentParser(description = text)  
    parser.add_argument("--collector","-c",help="Choose collector to push data for")
    parser.add_argument("--startTime","-s",help="Choose start time (Format: Y-m-dTH:M:S; Example: 2017-11-06T16:00:00)")
    parser.add_argument("--endTime","-e",help="Choose end time (Format: Y-m-dTH:M:S; Example: 2017-11-06T16:00:00)")
    parser.add_argument("--type","-t",help="Choose record type: ribs or updates")

    args = parser.parse_args() 

    # initialize recordType
    recordType = ""
    if args.type:
        if args.type in ["ribs", "updates"]:
            recordType = args.type
        else:
            sys.exit("Incorrect type specified; Choose from rib or update")
    else:
        sys.exit("Record type not specified")

    # initialize collector
    if args.collector:
        collector = args.collector
    else:
        sys.exit("Collector(s) not specified")

    # initialize time to start
    timeWindow = 15
    currentTime = datetime.utcnow()
    minuteStart = int(currentTime.minute/timeWindow)*timeWindow
    timeStart = ""
    if args.startTime:
        timeStart = args.startTime
    else:
        if recordType == 'updates':
            timeStart = currentTime.replace(microsecond=0, second=0, minute=minuteStart)-timedelta(minutes=3*timeWindow)
        else:
            delay = 120
            if 'rrc' in collector:
                delay = 480
            timeStart = currentTime-timedelta(minutes=delay)

    # initialize time to end
    timeEnd = ""
    if args.endTime:
        timeEnd = args.endTime
    else:
        if recordType == 'updates':
            timeEnd = currentTime.replace(microsecond=0, second=0, minute=minuteStart)-timedelta(minutes=2*timeWindow)
        else:
            timeEnd = currentTime


    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
            format=FORMAT, filename='log/ihr-kafka-bgpstream2_{}.log'.format(collector) , 
            level=logging.WARN, datefmt='%Y-%m-%d %H:%M:%S'
            )
    logging.warning("Started: %s" % sys.argv)
    logging.warning("Arguments: %s" % args)
    logging.warning('start time: {}, end time: {}'.format(timeStart, timeEnd))

    logging.warning("Downloading {} data for {}".format(recordType, collector))
    pushData(recordType, collector, timeStart, timeEnd)
        
    logging.warning("End: %s" % sys.argv)
