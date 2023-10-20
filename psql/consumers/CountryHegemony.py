import os
import sys
import psycopg2
import psycopg2.extras
from pgcopy import CopyManager
from confluent_kafka import Consumer 
import logging
from collections import defaultdict
import json
import msgpack
from datetime import datetime
from iso3166 import countries


class saverPostgresql(object):
    """Dumps hegemony results to a Postgresql database. """

    def __init__(self, af, host="localhost", dbname="ihr"):

        self.prevts = 0 
        self.af = int(af)
        self.dataHege = [] 
        self.cpmgr = None
        self.continents = {
                'EU': 'European Union',
                'AP': 'Asia-Pacific'
                }

        self.conn = psycopg2.connect(DB_CONNECTION_STRING)
        columns=("timebin", "country_id", "asn_id", "hege", "af", "weight", "weightscheme", "transitonly")
        self.cpmgr = CopyManager(self.conn, 'ihr_hegemony_country', columns)
        self.cursor = self.conn.cursor()
        logging.debug("Connected to the PostgreSQL server")

        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_HOST,
            'group.id': 'ihr_hegemony_countries_psql_sink_ipv{}'.format(self.af),
            'auto.offset.reset': 'earliest',
            })

        self.consumer.subscribe(['ihr_hegemony_countries_ipv{}'.format(self.af)])

        self.updateCountries()

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

    def updateCountries(self):
        '''
        Get the list of countries from the database
        '''

        self.cursor.execute("SELECT code FROM ihr_country")
        self.countries = set([x[0] for x in self.cursor.fetchall()])
        logging.debug("%s counties registered in the database" % len(self.countries))


    def save(self, msg):
        """
        Buffer the given message and  make sure corresponding ASNs are 
        registered in the database.
        """

        if 'ts' not in msg:
            print(msg)
        # Update the current bin timestamp
        if self.prevts != msg['ts']:
            self.commit()
            self.prevts = msg['ts']
            self.currenttime = datetime.utcfromtimestamp(msg['ts'])
            logging.debug("start recording country hegemony")


        # Update seen countries
        if msg['cc'] not in self.countries:
            self.countries.add(msg['cc'])
            if msg['cc'] in self.continents:
                country_name = self.continents[msg['cc']]
            else:
                country_name = countries.get(msg['cc']).name

            logging.warning("psql: add new country %s: %s" % (msg['cc'], country_name))
            self.cursor.execute(
                    "INSERT INTO ihr_country(code, name, tartiflette, disco ) \
                            select %s, %s, FALSE, FALSE \
                            WHERE NOT EXISTS ( SELECT code FROM ihr_country WHERE code = %s)", 
                            (msg['cc'], country_name, msg['cc']))

        # Hegemony values to copy in the database
        if msg['hege']!= 0:
            self.dataHege.append((self.currenttime, msg['cc'], int(msg['asn']), 
                float(msg['hege']), self.af, msg['original_weight'], msg['weight'].lower(), msg['transit_only']))


    def commit(self):
        """
        Push buffered messages to the database and flush the buffer.
        """

        if len(self.dataHege) == 0:
            return

        logging.warning("psql: start copy")
        self.cpmgr.copy(self.dataHege)
        self.conn.commit()
        logging.warning("psql: end copy")

        self.dataHege = []
        self.updateCountries()


if __name__ == "__main__":
    if len(sys.argv)<2:
        print("usage: %s af" % sys.argv[0])
        sys.exit()

    logging.basicConfig(
            format='%(asctime)s %(processName)s %(message)s',
            level=logging.info,
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[logging.StreamHandler()])

    global KAFKA_HOST
    KAFKA_HOST = os.environ["KAFKA_HOST"]
    global DB_CONNECTION_STRING
    DB_CONNECTION_STRING = os.environ["DB_CONNECTION_STRING"]

    logging.warning("Started: %s" % sys.argv)

    af = int(sys.argv[1])
    ss = saverPostgresql(af)
    ss.run()

