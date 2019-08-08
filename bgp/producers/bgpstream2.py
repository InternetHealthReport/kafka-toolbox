import sys
import argparse
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from pybgpstream import BGPStream
from datetime import datetime
from datetime import timedelta
import msgpack
import logging


def dt2ts(dt):
    return int((dt - datetime(1970, 1, 1)).total_seconds())


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

    topicName = "ihr_bgp_" + collector + "_" + record_type
    admin_client = KafkaAdminClient(
            bootstrap_servers=['kafka1:9092', 'kafka2:9092', 'kafka3:9092'], 
            client_id='bgp_producer_admin')

    try:
        topic_list = [NewTopic(name=topicName, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except:
        pass
    admin_client.close()

    producer = KafkaProducer(
        bootstrap_servers=['kafka1:9092', 'kafka2:9092', 'kafka3:9092'], 
        # acks=0, 
        value_serializer=lambda v: msgpack.packb(v, use_bin_type=True),
        linger_ms=1000, request_timeout_ms=300000, compression_type='snappy')
    
    for rec in stream.records():
        completeRecord = {}
        completeRecord["rec"] = getRecordDict(rec)
        completeRecord["elements"] = []

        recordTimeStamp = rec.time

        recordTimeStamp = int(recordTimeStamp) * 1000

        for elem in rec:
            elementDict = getElementDict(elem)
            completeRecord["elements"].append(elementDict)

        producer.send(topicName, completeRecord, timestamp_ms=recordTimeStamp)

    producer.close()


if __name__ == '__main__':

    text = "This script pushes BGP data from specified collector(s) \
for the specified time window to Kafka topic(s). The created topics have only \
one partition in order to make sequential reads easy. If no start and end time \
is given then it download data for the current hour."

    parser = argparse.ArgumentParser(description = text)  
    parser.add_argument("--collector","-c",help="Choose collector(s) to push data for")
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

    # initialize collectors
    collectors = []
    if args.collector:
        collectorList = args.collector.split(",")
        collectors = collectorList
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
            timeStart = currentTime.replace(microsecond=0, second=0, minute=minuteStart)-timedelta(minutes=2*timeWindow)
        else:
            timeStart = currentTime-timedelta(minutes=60)

    # initialize time to end
    timeEnd = ""
    if args.endTime:
        timeEnd = args.endTime
    else:
        if recordType == 'updates':
            timeEnd = currentTime.replace(microsecond=0, second=0, minute=minuteStart)-timedelta(minutes=timeWindow)
        else:
            timeEnd = currentTime


    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(format=FORMAT, filename='ihr-kafka-bgpstream2.log' % timeStart, level=logging.ERROR, datefmt='%Y-%m-%d %H:%M:%S')
    logging.info("Started: %s" % sys.argv)
    logging.info("Arguments: %s" % args)
    logging.warning('start time: {}, end time: {}'.format(timeStart, timeEnd))

    for collector in collectors:
        logging.warning("Downloading {} RIB data for {}".format(recordType, collector))
        pushData(recordType, collector, timeStart, timeEnd)
        
