import sys
import msgpack 
import argparse
from confluent_kafka import Consumer, TopicPartition, KafkaError


if __name__ == '__main__':

    text = "Retrieve and print the last n messages in the given topic"

    parser = argparse.ArgumentParser(description = text)  
    parser.add_argument("--topic","-t",help="Selected topic")
    parser.add_argument("--num_msg","-n",help="Number of messages to print", default=1, type=int)
    parser.add_argument("--server","-s",help="Bootstrap server", default='kafka1:9092')
    parser.add_argument("--partition","-p",help="Partition number", type=int, default=0)

    args = parser.parse_args() 

    assert args.num_msg > 0

    # Instantiate Kafka Consumer
    # consumer = KafkaConsumer(args.topic, bootstrap_servers=[args.server],
            # group_id='ihr_tail', enable_auto_commit=False, consumer_timeout_ms=10000,
            # value_deserializer=lambda v: msgpack.unpackb(v, raw=False))
    consumer = Consumer({
        'bootstrap.servers':args.server,
        'group.id': 'ihr_tail',
        'enable.auto.commit': False,
            })
    partition = TopicPartition(args.topic, args.partition)
    low, high = consumer.get_watermark_offsets(partition)
    offset = high - args.num_msg
    print(low,high,offset)

    if offset<0 and args.num_msg==1:
        sys.exit('Empty topic')
    elif offset < 0:
        print("reading the entire topic")
        partition = TopicPartition(args.topic, args.partition, low)
    else:
        partition = TopicPartition(args.topic, args.partition, offset)

    consumer.assign([partition])

    # retrieve messages
    # for i, message in enumerate(consumer.poll(1000)):
        # print(message)
        # if i>=args.num_msg-1:
            # break
    i = 0
    while True:

        msg= consumer.poll(1000)
        msgdict = { 
                'topic': msg.topic(),
                'partition': msg.partition(),
                'key': msg.key(),
                'timestamp': msg.timestamp(),
                'headers': msg.headers(),
                'value': msgpack.unpackb(msg.value(), raw=False)
                }
        print(msgdict)

        i += 1
        if i >= args.num_msg:
            break

    consumer.close()
