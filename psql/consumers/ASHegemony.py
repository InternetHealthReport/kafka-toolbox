import sys
import psycopg2
import psycopg2.extras
from pgcopy import CopyManager
from confluent_kafka import Consumer, TopicPartition, KafkaError
import logging
from collections import defaultdict
import json
import msgpack
from datetime import datetime
import arrow

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

    def __init__(self, topic, af, start=None, host="localhost", dbname="ihr"):

        self.prevts = 0 
        # TODO: get names from Kafka 
        self.asNames = defaultdict(str, json.load(open("/home/romain/Projects/perso/ashash/data/asNames.json")))
        self.af = int(af)
        self.dataHege = [] 
        self.hegemonyCone = defaultdict(int)
        self.partition_total = 0
        self.partition_paused = 0
        self.cpmgr = None

        conn_string = "host='127.0.0.1' dbname='%s'" % dbname

        self.conn = psycopg2.connect(conn_string)
        columns=("timebin", "originasn_id", "asn_id", "hege", "af")
        self.cpmgr = CopyManager(self.conn,topic, columns)
        self.cursor = self.conn.cursor()
        logging.debug("Connected to the PostgreSQL server")

        self.consumer = Consumer({
            'bootstrap.servers': 'kafka1:9092, kafka2:9092, kafka3:9092',
            'group.id': 'ihr_psql_sink_ipv{}'.format(self.af),
            'auto.offset.reset': 'earliest',
            'fetch.min.bytes': 100000,
            })

        if start is None:
            self.consumer.subscribe([topic])
            self.partitions = None
        else:
            timestamp = int(arrow.get(start).timestamp())
            timestamp_ms = timestamp * 1000
            topic_info = self.consumer.list_topics(topic)
            partitions = [TopicPartition(topic, partition_id, timestamp_ms) 
                    for partition_id in  topic_info.topics[topic].partitions.keys()]

            self.partitions = self.consumer.offsets_for_times( partitions )
            self.consumer.assign(self.partitions)


        self.partition_paused = 0
        self.buffer = []


        self.updateASN()

    def run(self):
        """
        Consume data from the kafka topic and save it to the database.
        """

        while True:
            msg = self.consumer.poll(10.0)

            if self.partitions is None:
                self.partitions = self.consumer.assignment()

            if msg is None:
                continue

            if msg.error():
                logging.error("Consumer error: {}".format(msg.error()))
                continue

            msg_val = msgpack.unpackb(msg.value(), raw=False)

            # Update the current bin timestamp
            if self.prevts != msg_val['timestamp']:

                self.consumer.pause([TopicPartition(msg.topic(), msg.partition())])
                self.partition_paused += 1
                self.buffer.append(msg_val)

                # all partition are paused, commit data
                if self.partition_paused == len(self.partitions):
                    self.commit()
                    self.prevts = msg_val['timestamp']
                    self.currenttime = datetime.utcfromtimestamp(msg_val['timestamp'])

                    # process buffer's messages
                    for msg_buf in self.buffer:
                        self.save(msg_buf)
                    self.buffer = []

                    # resume all partition
                    self.partition_paused = 0
                    self.consumer.resume(self.partitions)

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
        
        if msg['scope'] == '-1':
            msg['scope'] = '0'

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

        for asn, hege in msg['scope_hegemony'].items():
            if int(asn) not in self.asns:
                self.asns.add(int(asn))
                logging.warning("psql: add new asn %s" % asn)
                self.cursor.execute(
                        "INSERT INTO ihr_asn(number, name, tartiflette, disco, ashash) \
                                select %s, %s, FALSE, FALSE, TRUE \
                                WHERE NOT EXISTS ( SELECT number FROM ihr_asn WHERE number = %s)", 
                                (asn, self.asNames["AS"+str(asn)], asn))
                self.cursor.execute("UPDATE ihr_asn SET ashash = TRUE where number = %s", (int(asn),))

            # Hegemony values to copy in the database
            if hege!= 0:
                self.dataHege.append((self.currenttime, int(msg['scope']), int(asn), float(hege), self.af))

            # Compute Hegemony cone size
            asn = int(asn)
            scope = int(msg['scope'])
            inc = 1
            if scope == -1 or asn == scope or hege==0:
                # ASes with empty cone are still stored
                inc = 0

            self.hegemonyCone[asn] += inc

    def commit(self):
        """
        Push buffered messages to the database and flush the buffer.
        """

        if len(self.dataHege) == 0:
            return

        logging.warning(f"psql: start copy, ts={self.currenttime}, nb. data points={len(self.dataHege)}")
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
        print("usage: %s topic af [starttime]" % sys.argv[0])
        sys.exit()

    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(format=FORMAT, filename='ihr-kafka-psql-ASHegemony.log', level=logging.WARN, datefmt='%Y-%m-%d %H:%M:%S')
    logging.warning("Started: %s" % sys.argv)

    topic = int(sys.argv[1])
    af = int(sys.argv[2])
    start = None
    if len(sys.argv) > 3:
        start = sys.argv[3]

    ss = saverPostgresql(topic, af, start)
    ss.run()

