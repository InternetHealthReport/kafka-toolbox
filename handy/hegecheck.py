import sys
import msgpack 
import argparse
import arrow
from confluent_kafka import Consumer, TopicPartition, KafkaError
import confluent_kafka 


if __name__ == '__main__':

    text = "Ouput stats for a certain timebin"

    parser = argparse.ArgumentParser(description = text)  
    parser.add_argument("--topic","-t",help="Selected topic")
    parser.add_argument("--server","-s",help="Bootstrap server", default='kafka1:9092')
    parser.add_argument("--timebin","-b",help="Time bin", default='2021-03-08T00:00')

    args = parser.parse_args() 

    consumer = Consumer({
        'bootstrap.servers': args.server,
        'group.id': 'ihr_hegecheck',
        'session.timeout.ms': 600000,
        'max.poll.interval.ms': 600000,
        'fetch.min.bytes': 100000,
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest'
    })

    timestamp = int(arrow.get(args.timebin).timestamp())
    timestamp_ms = timestamp * 1000
    nb_stopped_partitions = 0
    partitions = []
    topic_info = consumer.list_topics(args.topic)
    partitions = [TopicPartition(args.topic, partition_id, timestamp_ms) 
            for partition_id in  topic_info.topics[args.topic].partitions.keys()]

    time_offset = consumer.offsets_for_times( partitions )
    consumer.assign(time_offset)

    i = 0
    scopes = set()
    asns = set()
    pairs = set()
    nb_messages = 0
    global_graph = set()
    while True:

        bmsg = consumer.poll(1000)

        if bmsg is None:
            logging.error(f"consumer timeout")
            break

        if bmsg.error():
            logging.error(f"consumer error {bmsg.error()}")
            continue

        # Filter with start and end times
        ts = bmsg.timestamp()

        if ts[0] == confluent_kafka.TIMESTAMP_CREATE_TIME and ts[1] > timestamp_ms:
            consumer.pause([TopicPartition(bmsg.topic(), bmsg.partition())])
            nb_stopped_partitions += 1
            if nb_stopped_partitions < len(time_offset):
                continue
            else:
                break

        nb_messages += 1
        msg = msgpack.unpackb(bmsg.value(), raw=False)
        scopes.add(msg['scope']) 
        for asn in msg['scope_hegemony'].keys():
            asns.add(asn)
            pairs.add( (msg['scope'], asn) )

            if msg['scope'] == '-1':
                global_graph.add(asn)


    consumer.close()

    print(f"Stats for timebin {args.timebin}")
    print(f'\t- nb. messages: {nb_messages}')
    print(f'\t- nb. scopes: {len(scopes)}')
    print(f'\t- nb. asns: {len(asns)}')
    print(f'\t- global graph size: {len(global_graph)}')
    print(f'\t- nb. pairs (scope, asn): {len(pairs)}')

