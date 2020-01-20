import sys
from collections import defaultdict
import configparser
import logging
from datetime import datetime
import json
import msgpack
import statistics
from confluent_kafka import Consumer, Producer, TopicPartition, KafkaError
import confluent_kafka

# TODO: how to handle data holes?

# Requirements: 
# - rely only on kafka timestamps, make sure kafka timestamps correspond to the datapoint timestamps
# - assume the input topic is temporaly sorted

MIN_HIST_SIZE = 2/3

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        # print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        pass


class AnomalyDetector():

    def __init__(self, conf_fname='anomalyDetection.conf'):
        """ Read the configuration file and initialize parameter values.
        Does not initialize the history. Run init_history() before starting
        detect()"""
        
        # Read the config file
        config = configparser.ConfigParser()
        config.read(conf_fname)

        self.detection_threshold = config.get('detection', 'threshold')
        self.detection_noise = config.get('detection', 'noise')
        self.history_hours = config.get('detection', 'history_hours')

        self.kafka_topic_in = config.get('io', 'input_topic')
        self.value_field = config.get('io', 'value_field')
        self.key_field = config.get('io', 'key_field')
        self.time_granularity_min = config.get('io', 'time_granularity_min')
        self.kafak_topic_out = config.get('io', 'output_topic')

        self.history = defaultdict(lambda : {'values':[], 'timestamps':[]})

        # Initialize kafka consumer
        self.consumer = Consumer({
            'bootstrap.servers': 'kafka1:9092, kafka2:9092, kafka3:9092',
            'group.id': 'ihr_anomaly_detector_{}'.format(self.kafka_topic_in),
            'auto.offset.reset': 'earliest',
            })

        self.consumer.subscribe([self.kafka_topic_in])
        self.detection_starttime = self.get_current_timestamp()
        logging.debug('Detection starttime set to: {}'.format(self.detection_starttime))

        # Initialize kafka producer
        self.producer = Producer({'bootstrap.servers': 'kafka1:9092,kafka2:9092,kafka3:9092',
            'default.topic.config': {'compression.codec': 'snappy'}}) 



    def get_current_timestamp(self):
        """Retrieve the timestamp of the next message in the kafka input topic"""

        msg = self.consumer.poll()

        if msg.error():
            logging.error("Consumer error while getting the current timestamp: {}"
                    .format(msg.error()))
            return None

        ts = msg.timestamp()

        if ts[0] == confluent_kafka.TIMESTAMP_CREATE_TIME:
            return ts[1]
        else:
            return None


    def init_history(self):
        """Populate the history with data preceding the detection time."""

        history_starttime = self.detection_starttime - (self.history_hours*60*60*1000) 
        logging.debug('History starttime set to: {}'.format(history_starttime))
        
        # Set offsets according to current data and history size
        topic_info = self.consumer.list_topics(self.kafka_topic_in)
        partitions = [TopicPartition(self.kafka_topic_in, partition_id, history_starttime) 
                for partition_id in  topic_info.topics[self.topic].partitions.keys()]

        for offset in self.consumer.offsets_for_times(partitions):
            self.consumer.seek(offset)

        logging.warning('Fetching historical data...')
        timestamp = 0
        while True:
            msg = self.consumer.poll()

            if msg.error():
                logging.error("Consumer error: {}".format(msg.error()))
                continue

            # Stop when we get to the detection timestamp
            ts = msg.timestamp()
            if ts[0] == confluent_kafka.TIMESTAMP_CREATE_TIME and ts[1] >= self.detection_starttime:
                break

            # Populate history dictionary
            datapoint = msgpack.unpackb(msg.value(), raw=False)

            self.history[datapoint[self.key_field]]['values'].append(datapoint[self.value_field])
            self.history[datapoint[self.key_field]]['timestamps'].append(ts[1])


    def detect(self):
        """
        Consume data from kafka topic, report anomalous datapoint, and update history.
        """

        while True:
            msg = self.consumer.poll(10.0)
            if msg is None:
                continue

            if msg.error():
                logging.error("Consumer error: {}".format(msg.error()))
                continue

            datapoint = msgpack.unpackb(msg.value(), raw=False)
            ts = msg.timestamp()

            hist = self.history[datapoint[self.key_field]]

            # Remove outdated values from the history
            while hist['timestamps'][0] < ts[1]-self.history_hours*60*60*1000:
                hist['timestamps'].pop(0)
                hist['values'].pop(0)

            # Check if we have at least MIN_HIST_SIZE of expected data in the history
            if len(hist['values']) < MIN_HIST_SIZE * self.history_hours * (60/self.time_granularity):
                continue

            # Compute detection boundaries
            median = statistics.median(hist['values'])
            mad = 1.4826*statistics.median([statistics.fabs(x-median) for x in hist['values']])

            # Check if the new datapoint is within the boundaries
            deviation = (datapoint[self.value_field] - median) / (mad*self.detection_threshold+median*self.noise)
            if deviation > self.detection_threshold or deviation < self.detection_threshold:
                self.report_anomaly(ts[1], datapoint, deviation)

            # Add new datapoint to the history
            hist['values'].append(datapoint[self.value_field])
            hist['timestamps'].append(ts[1])


    def report_anomaly(self, timestamp, datapoint, deviation):
        """Report anomalous value to Kafka."""

        logging.debug('Report anomalous datapoint: {}, {}'.format(datapoint, deviation))

        producer.produce(
                self.kafak_topic_out, 
                msgpack.packb({'datapoint':datapoint, 'deviation':deviation}, use_bin_type=True), 
                callback=delivery_report,
                timestamp = timestamp
                )

        logging.debug('produced anomalous report')
        # Trigger any available delivery report callbacks from previous produce() calls
        producer.poll(0)


if __name__ == "__main__":
    if len(sys.argv)<2:
        print("usage: %s config_file" % sys.argv[0])
        sys.exit()

    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(format=FORMAT, filename='ihr-kafka-detector.log', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
    logging.warning("Started: %s" % sys.argv)

    conf_fname = sys.argv[1]
    detector = AnomalyDetector(conf_fname)
    detector.init_history()
    detector.detect()
    
