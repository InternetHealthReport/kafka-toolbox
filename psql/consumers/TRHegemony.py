import argparse
import configparser
import logging
import os
import sys
from datetime import datetime
from logging.config import fileConfig

import msgpack
import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer
from pgcopy import CopyManager


class saverTRHegemony(object):

    """Dumps traceroute hegemony results to the Postgresql database. """

    def __init__(self, config_file):
        """ Initialization:
        - Connects to postgresql
        - Fetch current identifiers
        - Subscribe to the kafka topic
        """

        # Initialize logger
        fileConfig(config_file)
        logging.info("Started: {}".format(sys.argv))

        # Read the config file
        config = configparser.ConfigParser()
        config.read(config_file)

        self.prevts = 0
        self.currenttimebin = None
        self.data = []
        self.cpmgr = None
        kafka_topic = config.get('kafka', 'input_topic')
        kafka_consumer_group = config.get('kafka', 'consumer_group')
        self.af = config.getint('kafka', 'input_af')
        if self.af not in (4, 6):
            logging.error(f'Invalid address family specified: {self.af}')
            raise ValueError(f'Invalid address family specified: {self.af}')

        table = config.get('psql', 'table')
        columns = [column for column in config.get('psql', 'columns').split(',')]



        self.conn = psycopg2.connect(DB_CONNECTION_STRING)
        self.cpmgr = CopyManager(self.conn, table, columns)
        self.cursor = self.conn.cursor()
        logging.debug("Connected to the PostgreSQL server")

        self.cursor.execute("SELECT id, type, name, af FROM ihr_tr_hegemony_identifier")
        self.identifiers = {tuple(x[1:]): x[0] for x in self.cursor.fetchall()}
        logging.debug("%s identifiers registered in the database" % len(self.identifiers))

        # Kafka consumer initialization
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_HOST,
            'group.id': kafka_consumer_group,
            'auto.offset.reset': 'earliest',
        })

        self.consumer.subscribe([kafka_topic])

        self.run()

    def transform_identifier(self, identifier: str):
        if identifier.startswith('as|'):
            return ('AS', identifier.removeprefix('as|'), self.af)
        if identifier.startswith('ip|'):
            return ('IP', identifier.removeprefix('ip|'), self.af)
        if not identifier.startswith('ix|'):
            logging.error(f'Invalid identifier: {identifier}')
            return tuple()
        # Must be IXP or IXP member entry, both starting with 'ix|'.
        if ';' in identifier:
            ix_id, asn = identifier.split(';')
            ix_id = ix_id.removeprefix('ix|')
            asn = asn.removeprefix('as|')
            if asn == '0':
                # Member interface IP could not be mapped to an ASN.
                return tuple()
            return ('MB', f'{ix_id};{asn}', self.af)
        return ('IX', identifier.removeprefix('ix|'), self.af)

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
            self.save(msg_val)

    def save(self, msg):
        """
        Buffer the given message and make sure corresponding identifiers are
        registered in the database.
        """

        # Traceroute hegemony dumps need to be shifted by three weeks to get the real
        # timestamp. This is due to some hackery of getting the hegemony code to work
        # with RIBs created from traceroute data.
        # Without going into too much detail, when we produce scores for time x which
        # includes data from [x - 4 weeks, x], then the output timestamp in Kafka will
        # be x - 3 weeks, which is why we need to correct it here before putting it into
        # the database.
        # 3 weeks * 7 days * 24 hours * 60 minutes * 60 seconds
        msg['timestamp'] += 3 * 7 * 24 * 60 * 60

        # Update the current bin timestamp
        if self.prevts != msg['timestamp']:
            self.commit()
            self.prevts = msg['timestamp']
            self.currenttimebin = datetime.utcfromtimestamp(msg['timestamp'])
            logging.debug("start recording tr hegemony results (ts={})".format(self.currenttimebin))

        # Only push scores that are based on traceroutes from at least 10 probe ASes.
        nbsamples = msg['nb_peers']
        if nbsamples < 10:
            return

        origin_identifier = self.transform_identifier(msg['scope'])
        dependency_identifier = self.transform_identifier(msg['asn'])
        if not origin_identifier or not dependency_identifier:
            return

        new_identifiers = {origin_identifier, dependency_identifier}.difference(self.identifiers)
        if new_identifiers:
            logging.warning("psql: add new identifiers %s" % new_identifiers)
            insertQuery = 'INSERT INTO ihr_tr_hegemony_identifier (type, name, af) \
                    VALUES (%s, %s, %s) RETURNING id'
            for loc in new_identifiers:
                self.cursor.execute(insertQuery, loc)
                loc_id = self.cursor.fetchone()[0]
                self.identifiers[loc] = loc_id

        self.data.append((
            self.currenttimebin,
            self.identifiers[origin_identifier],
            self.identifiers[dependency_identifier],
            msg['hege'],
            self.af,
            nbsamples
        ))

    def commit(self):
        """
        Push buffered messages to the database and flush the buffer.
        """

        if not self.data:
            return

        logging.warning("psql: start copy")
        self.cpmgr.copy(self.data)
        self.conn.commit()
        logging.warning("psql: end copy")
        self.data = []


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s %(processName)s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[logging.StreamHandler()])

    global KAFKA_HOST
    KAFKA_HOST = os.environ["KAFKA_HOST"]
    global DB_CONNECTION_STRING
    DB_CONNECTION_STRING = os.environ["DB_CONNECTION_STRING"]

    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    args = parser.parse_args()

    sod = saverTRHegemony(args.config)
