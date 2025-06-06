import os
import time
import msgpack 
import arrow
from kafka import KafkaConsumer
from kafka.structs import TopicPartition

NB_RETRY = 3
BOOTSTRAP_SERVER = 'kafka1:9092'
collectors= [
        'route-views2', 
        'route-views.sydney', 
        'route-views.jinx', 
        'route-views.chicago', 
        'rrc04', 
        'rrc10', 
        'rrc11', 
        'rrc12', 
        'rrc13', 
        'rrc14', 
        'rrc15', 
        'rrc16', 
        'rrc19', 
        'rrc20', 
        'rrc23', 
        'rrc24'
        ]

ignore = ['rrc12', 'route-views.jinx', 'rrc00']

tails = {}


def check_times():
    for collector in collectors:
        topic = 'ihr_bgp_%s_ribs' % collector

        # Instantiate Kafka Consumer
        consumer = KafkaConsumer(topic, bootstrap_servers=[BOOTSTRAP_SERVER],
                group_id='ihr_rib_check', enable_auto_commit=False, 
                consumer_timeout_ms=10000,
                value_deserializer=lambda v: msgpack.unpackb(v, raw=False))
        partition = TopicPartition(topic, 0)

        # go to end of the stream with dummy poll
        consumer.poll()
        consumer.seek_to_end()
        offset = consumer.position(partition)-1

        if offset<0: 
            print(collector, ' empty topic!!')
        else:
            consumer.seek(partition, offset)

        date = None
        # retrieve messages
        for i, message in enumerate(consumer):
            date = arrow.get(message.timestamp)
            print(collector, ' ', date) 

        tails[collector] = date

        consumer.close()

def update_old_topics():
    
    updated_topics = False

    # Update out-of-sync topics
    latest = max([val for val in tails.values() if val is not None])

    print('Latest timestamp: ', latest)

    for collector, date in tails.items():
        if( collector not in ignore and
            ( date is None or (latest-date).total_seconds() > 43000 ) ):
            
            print('Downloading latest data for ', collector)
            os.system(
                'python3 bgpstream2.py -t ribs --collector %s --startTime %s --endTime %s' 
                % ( collector, latest.shift(hours=-1), latest.shift(hours=1))
                )
            updated_topics = True

    return updated_topics

if __name__ == '__main__':

    for i in range(NB_RETRY):
        check_times()
        updates = update_old_topics()

        if not updates:
            break
        elif i != NB_RETRY-1:
            time.sleep(900)

