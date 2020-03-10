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

def validASN(asn):
    if isinstance(asn,int):
        return True
    try:
        a = int(asn)
    except ValueError:
        return False

    return True


class saverPostgresql(object):
    """Dumps hegemony results to a Postgresql database. """

    def __init__(self, af, host="localhost", dbname="ihr"):

        self.prevts = 0 
        # TODO: get names from Kafka 
        self.asNames = defaultdict(str, json.load(open("/home/romain/Projects/perso/ashash/data/asNames.json")))
        self.af = int(af)
        self.dataHege = [] 
        self.hegemonyCone = defaultdict(int)
        self.cpmgr = None

        conn_string = "host='127.0.0.1' dbname='%s'" % dbname

        self.conn = psycopg2.connect(conn_string)
        columns=("timebin", "originasn_id", "asn_id", "hege", "af")
        self.cpmgr = CopyManager(self.conn, 'ihr_hegemony', columns)
        self.cursor = self.conn.cursor()
        logging.debug("Connected to the PostgreSQL server")

        self.consumer = Consumer({
            'bootstrap.servers': 'kafka1:9092, kafka2:9092, kafka3:9092',
            'group.id': 'ihr_hegemony_values_psql_sink_ipv{}'.format(self.af),
            'auto.offset.reset': 'earliest',
            })

        self.consumer.subscribe(['ihr_hegemony_values_ipv{}'.format(self.af)])

        self.updateASN()

    def run(self):
        """
        Consume data from the kafka topic and save it to the database.
        """

        while True:
            msg = self.consumer.poll(10.0)
            if msg is None:
                continue

            if msg.error():
                logging.error("Consumer error: {}".format(msg.error()))
                continue

            msg_val = msgpack.unpackb(msg.value(), raw=False)

            if not validASN(msg_val['asn']):
                if (isinstance(msg_val['asn'], str) 
                    and msg_val['asn'].startswith('{')
                    and msg_val['asn'].endswith('{')):

                    # This is an AS set, push results for each ASN in the set
                        for asn in msg_val['asn'][1:-1].split(','):
                            if validASN(asn):
                                msg_val['asn'] = asn
                                self.save(msg_val)

            else:
                self.save(msg_val)

    def updateASN(self):
        '''
        Get the list of ASNs from the database
        '''

        self.cursor.execute("SELECT number FROM ihr_asn WHERE ashash=TRUE")
        self.asns = set([x[0] for x in self.cursor.fetchall()])
        logging.debug("%s ASNS already registered in the database" % len(self.asns))


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
            logging.debug("start recording hegemony")


        # Update seen ASNs
        if int(msg['scope']) not in self.asns:
            self.asns.add(int(msg['scope']))
            logging.warning("psql: add new scope %s" % msg['scope'])
            self.cursor.execute(
                    "INSERT INTO ihr_asn(number, name, tartiflette, disco, ashash) \
                            select %s, %s, FALSE, FALSE, TRUE \
                            WHERE NOT EXISTS ( SELECT number FROM ihr_asn WHERE number = %s)", 
                            (msg['scope'], self.asNames["AS"+str(msg['scope'])], msg['scope']))
            self.cursor.execute("UPDATE ihr_asn SET ashash = TRUE where number = %s", (int(msg['scope']),))

        if int(msg['asn']) not in self.asns:
            self.asns.add(int(msg['asn']))
            logging.warning("psql: add new asn %s" % msg['asn'])
            self.cursor.execute(
                    "INSERT INTO ihr_asn(number, name, tartiflette, disco, ashash) \
                            select %s, %s, FALSE, FALSE, TRUE \
                            WHERE NOT EXISTS ( SELECT number FROM ihr_asn WHERE number = %s)", 
                            (msg['asn'], self.asNames["AS"+str(msg['asn'])], msg['asn']))
            self.cursor.execute("UPDATE ihr_asn SET ashash = TRUE where number = %s", (int(msg['asn']),))

        # Hegemony values to copy in the database
        if msg['hege']!= 0:
            self.dataHege.append((self.currenttime, int(msg['scope']), int(msg['asn']), float(msg['hege']), self.af))

        # Compute Hegemony cone size
        asn = int(msg['asn'])
        scope = int(msg['scope'])
        if asn != 0 and asn != scope:
            self.hegemonyCone[asn] += 1

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
        # Populate the table for AS hegemony cone
        logging.warning("psql: adding hegemony cone")
        
        data = [(self.currenttime, conesize, self.af, asn) 
                for asn, conesize in self.hegemonyCone.items() ]
        insert_query = 'INSERT INTO ihr_hegemonycone (timebin, conesize, af, asn_id) values %s'
        psycopg2.extras.execute_values (
            self.cursor, insert_query, data, template=None, page_size=100
        )

        # self.cursor.execute(
                # "INSERT INTO ihr_hegemonycone (timebin, conesize, af, asn_id) \
                        # SELECT timebin, count(distinct originasn_id), af, asn_id \
                        # FROM ihr_hegemony WHERE timebin=%s and asn_id!=originasn_id and originasn_id!=0 \
                        # GROUP BY timebin, af, asn_id;", (self.currenttime,))
        self.conn.commit()
        self.dataHege = []
        self.hegemonyCone = defaultdict(int)
        logging.warning("psql: end hegemony cone")

        self.updateASN()


if __name__ == "__main__":
    if len(sys.argv)<2:
        print("usage: %s af" % sys.argv[0])
        sys.exit()

    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(format=FORMAT, filename='ihr-kafka-psql-ASHegemony.log', level=logging.WARN, datefmt='%Y-%m-%d %H:%M:%S')
    logging.warning("Started: %s" % sys.argv)

    af = int(sys.argv[1])
    ss = saverPostgresql(af)
    ss.run()

