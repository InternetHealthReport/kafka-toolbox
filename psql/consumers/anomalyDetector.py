import sys
import psycopg2
import psycopg2.extras
from pgcopy import CopyManager
from confluent_kafka import Consumer 
import logging
from logging.config import fileConfig
from collections import defaultdict
import json
import msgpack
from datetime import datetime
import configparser

    
class saverPostgresql(object):
    """Dumps data from kafka topic to a Postgresql database. """

    def __init__(self, conf_fname, host="localhost", dbname="ihr"):

        self.prevts = 0 
        self.dataBuffer = [] 
        self.cpmgr = None


        # Read the config file
        config = configparser.ConfigParser()
        config.read(conf_fname)

        self.psql_host = config.get('psql', 'hostname')
        self.psql_dbname = config.get('psql', 'dbname')
        self.psql_table = config.get('psql', 'table')
        self.psql_columns = [column for column in config.get('psql', 'columns').split(',')]
        self.psql_columns_type = [key for key in config.get('psql', 'columns_type').split(',')]

        self.kafka_topic_in= config.get('kafka', 'input_topic')
        self.kafka_fields = [key for key in config.get('kafka', 'fields').split(',')]
        self.kafka_default_values = eval(config.get('kafka', 'default_values'))
        self.kafka_consumer_group = config.get('kafka', 'consumer_group')

        # Initialize logger
        fileConfig(conf_fname)
        logger = logging.getLogger()
        logging.info("Started: {}".format(sys.argv))

        # Connect to PostgreSQL
        conn_string = "host='{}' dbname='{}'".format(self.psql_host, self.psql_dbname)
        self.conn = psycopg2.connect(conn_string)
        self.cpmgr = CopyManager(self.conn, self.psql_table, self.psql_columns)
        self.cursor = self.conn.cursor()
        logging.debug("Connected to the PostgreSQL server")

        # Initialize Kafka consumer
        self.consumer = Consumer({
            'bootstrap.servers': 'kafka1:9092, kafka2:9092, kafka3:9092',
            'group.id': self.kafka_consumer_group,
            'auto.offset.reset': 'earliest',
            })

        self.consumer.subscribe([self.kafka_topic_in])

        # Load external data
        self.locations = None
        if 'atlas_location' in self.psql_columns_type:
            self.loadAtlasLocations()

    def loadAtlasLocations(self):
        """Fetch the unique id of each location"""

        self.cursor.execute("SELECT id, name, type, af FROM ihr_atlas_location")
        self.locations = {'{}{}v{}'.format(x[2],x[1],x[3]): x[0] for x in self.cursor.fetchall() }
        logging.debug("%s locations registered in the database" % len(self.locations))

    def cast(self, value, type):
        """Return the given value in the given type.

        For example, cast('213', 'int') returns 213"""

        if type == 'int':
            return int(value)
        elif type == 'float':
            return float(value)
        elif type == 'str':
            return str(value)
        elif type == 'datetime':
            return datetime.utcfromtimestamp(value)
        elif type == 'atlas_location':
            if value.endswith('v4') or value.endswith('v6'):
                return self.locations[value]
            else:
                return self.locations[value+'v4']


    def run(self):
        """
        Consume data from the kafka topic and save it to the database.
        """

        while True:
            msg = self.consumer.poll(10.0)
            if msg is None:
                self.commit()
                continue

            if msg.error():
                logging.error("Consumer error: {}".format(msg.error()))
                continue

            msg_val = msgpack.unpackb(msg.value(), raw=False)
            self.save(msg.timestamp()[1], msg_val)


    def save(self, ts, msg):
        """
        Buffer the given message and  make sure corresponding ASNs are 
        registered in the database.
        """

        # Update the current bin timestamp
        if self.prevts < ts:
            self.commit()
            self.prevts = ts
            logging.debug("start recording")

        row = []
        try:
            for field, field_type in zip(self.kafka_fields, self.psql_columns_type):
                if field in msg:
                    row.append(self.cast(msg[field], field_type))
                elif 'datapoint' in msg and field in msg['datapoint']:
                    row.append(self.cast(msg['datapoint'][field], field_type))
                elif field in self.kafka_default_values:
                    row.append(self.cast(self.kafka_default_values[field], field_type))
                else:
                    logging.error('Missing field {} in {}'.format(field, msg))
                    return 
        except ValueError:
            logging.error("Error invalid values: {}".format(msg))
            return

        self.dataBuffer.append(row)

    def commit(self):
        """
        Push buffered messages to the database and flush the buffer.
        """

        if len(self.dataBuffer) == 0:
            return

        logging.warning("psql: start copy")
        self.cpmgr.copy(self.dataBuffer)
        self.conn.commit()
        logging.warning("psql: end copy")
        self.dataBuffer = []

        if self.locations is not None:
            # Reload new locations
            self.loadAtlasLocations()


if __name__ == "__main__":
    if len(sys.argv)<2:
        print("usage: %s config.ini" % sys.argv[0])
        sys.exit()

    conf_fname = sys.argv[1]
    ss = saverPostgresql(conf_fname)
    ss.run()

