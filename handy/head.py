import sys
import msgpack 
import argparse
from confluent_kafka import Consumer, KafkaError


if __name__ == '__main__':

    text = "Retrieve and print the last n messages in the given topic"

    parser = argparse.ArgumentParser(description = text)  
    parser.add_argument("--topic","-t",help="Selected topic")
    parser.add_argument("--num_msg","-n",help="Number of messages to print", default=1, type=int)
    parser.add_argument("--server","-s",help="Bootstrap server", default='kafka1:9092')
    parser.add_argument("--partition","-p",help="Partition number", type=int, default=0)

    args = parser.parse_args() 

    # Instantiate Kafka Consumer
    c = Consumer({
        'bootstrap.servers': 'kafka1,kafka2,kafka3',
        'group.id': 'ihr_head',
        'auto.offset.reset': 'earliest',
    })
    c.subscribe([args.topic])

    nb_read = 0
    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        nb_read += 1
        print(msgpack.unpackb(msg.value(), raw=False))

        if nb_read >= args.num_msg:
            break

    c.close()

