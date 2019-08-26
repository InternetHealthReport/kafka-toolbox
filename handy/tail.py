import sys
import msgpack 
import argparse
from kafka import KafkaConsumer
from kafka.structs import TopicPartition


if __name__ == '__main__':

    text = "Retrieve and print the last n messages in the given topic"

    parser = argparse.ArgumentParser(description = text)  
    parser.add_argument("--topic","-t",help="Selected topic")
    parser.add_argument("--num_msg","-n",help="Number of messages to print", default=1, type=int)
    parser.add_argument("--server","-s",help="Bootstrap server", default='kafka1:9092')
    parser.add_argument("--partition","-p",help="Partition number", type=int, default=0)

    args = parser.parse_args() 

    # Instantiate Kafka Consumer
    consumer = KafkaConsumer(args.topic, bootstrap_servers=[args.server],
            group_id='ihr_tail', enable_auto_commit=True,
            value_deserializer=lambda v: msgpack.unpackb(v, raw=False))
    partition = TopicPartition(args.topic, args.partition)

    # go to end of the stream with dummy poll
    consumer.poll()
    consumer.seek_to_end()
    offset = consumer.position(partition)-args.num_msg

    if offset<0:
        sys.exit('Empty topic')

    # retrieve messages
    consumer.seek(partition, offset)
    for i, message in enumerate(consumer):
        print(message)
        if i>=args.num_msg-1:
            break

    consumer.close()
