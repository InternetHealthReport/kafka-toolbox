import sys
import psycopg2
import psycopg2.extras
from pgcopy import CopyManager
from kafka import KafkaConsumer
import logging
from collections import defaultdict
import json
import apsw
import glob
from datetime import datetime
from multiprocessing import JoinableQueue as mpQueue
from multiprocessing import Process
from cStringIO import StringIO

def validASN(asn):
    if isinstance(asn,int):
        return True
    try:
        a = int(asn)
    except ValueError:
        return False

    return True


class saverPostgresql(object):

    """Dumps only hegemony results to a Postgresql database. """

    def __init__(self, starttime, af, saverQueue, host="localhost", dbname="ihr"):

        self.prevts = 0 
        # TODO: get names from Kafka 
        self.asNames = defaultdict(str, json.load(open("data/asNames.json")))
        self.currenttime = starttime
        self.af = af
        self.dataHege = [] 
        self.cpmgr = None

        conn_string = "host='127.0.0.1' dbname='%s'" % dbname

        self.conn = psycopg2.connect(conn_string)
        columns=("timebin", "originasn_id", "asn_id", "hege", "af")
        self.cpmgr = CopyManager(self.conn, 'ihr_hegemony_test', columns)
        self.cursor = self.conn.cursor()
        logging.debug("Connected to the PostgreSQL server")

        self.updateASN()
        self.run()

    def run(self):

        self.consumer = KafkaConsumer(
                bootstrap_servers=['kafka1:9092', 'kafka2:9092', 'kafka3:9092'],
                auto_offset_reset='earliest',
                key_deserializer=lambda k: int.from_bytes(k, byteorder='big'),
                value_deserializer=lambda v: msgpack.unpackb(v, raw=False),
                group_id='ihr_hegemony_reader',
                )

        self.consumer.subscribe('ihr_hegemony_values_ipv{}'.format(self.af))

        while True:
            msg_pack = consumer.poll(10.0)
            if msg is None:
                self.commit()
                continue
            else:
                msg = msgpack.unpackb(msg_pack.value(), raw=False)
                for tp, msgs in msg.items():
                    for message in msgs:
                        self.save(message.value)

    def updateASN(self):

        self.cursor.execute("SELECT number FROM ihr_asn WHERE ashash=TRUE")
        self.asns = set([x[0] for x in self.cursor.fetchall()])
        logging.debug("%s ASNS already registered in the database" % len(self.asns))


    def save(self, msg):

        # Update the current bin timestamp
        if self.prevts != msg['ts']:
            self.prevts = msg['ts']
            self.currenttime = datetime.utcfromtimestamp(msg['ts'])
            logging.debug("start recording hegemony")

        # Update seen ASNs
        if int(msg['scope']) not in self.asns:
            self.asns.add(int(msg['scope']))
            logging.warn("psql: add new scope %s" % msg['scope'])
            self.cursor.execute(
                    "INSERT INTO ihr_asn(number, name, tartiflette, disco, ashash) \
                            select %s, %s, FALSE, FALSE, TRUE \
                            WHERE NOT EXISTS ( SELECT number FROM ihr_asn WHERE number = %s)", 
                            (msg['scope'], self.asNames["AS"+str(msg['scope'])], msg['scope']))
            self.cursor.execute("UPDATE ihr_asn SET ashash = TRUE where number = %s", (int(msg['scope']),))

        if int(msg['asn']) not in self.asns:
            self.asns.add(int(msg['asn']))
            logging.warn("psql: add new asn %s" % msg['asn'])
            self.cursor.execute(
                    "INSERT INTO ihr_asn(number, name, tartiflette, disco, ashash) \
                            select %s, %s, FALSE, FALSE, TRUE \
                            WHERE NOT EXISTS ( SELECT number FROM ihr_asn WHERE number = %s)", 
                            (msg['asn'], self.asNames["AS"+str(msg['asn'])], msg['asn']))
            self.cursor.execute("UPDATE ihr_asn SET ashash = TRUE where number = %s", (int(msg['asn']),))

        # Hegemony values to copy in the database
        if validASN(msg['asn']) and v!= 0:
            self.dataHege.append((self.currenttime, int(msg['scope']), int(msg['asn']), float(msg['hege']), int(self.af)))

    def commit(self):
        if len(self.dataHege) == 0:
            return

        logging.warn("psql: start copy")
        self.cpmgr.copy(self.dataHege, StringIO)
        self.conn.commit()
        logging.warn("psql: end copy")
        # Populate the table for AS hegemony cone
        logging.warn("psql: adding hegemony cone")
        self.cursor.execute("INSERT INTO ihr_hegemonycone (timebin, conesize, af, asn_id) SELECT timebin, count(distinct originasn_id), af, asn_id FROM ihr_hegemony WHERE timebin=%s and asn_id!=originasn_id and originasn_id!=0 GROUP BY timebin, af, asn_id;", (self.currenttime,))
        self.conn.commit()
        self.dataHege = []
        logging.warn("psql: end hegemony cone")

        self.updateASN()


if __name__ == "__main__":
    if len(sys.argv)<2:
        print("usage: %s af" % sys.argv[0])
        sys.exit()

    logging.basicConfig(level=logging.DEBUG)

    af = int(sys.argv[1])
    directory="newResults%s/" % af
    
    for slf in glob.glob(directory+"*.sql"):
        logging.debug("File: %s" % slf)
        # slf = "results_2017-03-15 00:00:00.sql"
        data = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
        
        # Get data from the sqlite db
        conn = apsw.Connection(slf)
        cursor = conn.cursor()
        cursor.execute("SELECT scope, ts, asn, hege FROM hegemony ORDER BY scope")

        for scope, ts, asn, hege in cursor.fetchall():
            data[scope][ts][asn] = hege

        # Push data to PostgreSQL
        dt = slf.partition("_")[2]
        dt = dt.partition(" ")[0]
        ye, mo, da = dt.split("-")
        starttime = datetime(int(ye), int(mo), int(da))

        saverQueue = mpQueue(1000)
        ss = Process(target=saverPostgresql, args=(starttime, af, saverQueue), name="saverPostgresql")
        ss.start()

        saverQueue.put("BEGIN TRANSACTION;")
        for scope, allts in data.iteritems():
            for ts, hege in allts.iteritems():
                saverQueue.put(("hegemony", (ts, scope, hege)) )
        saverQueue.put("COMMIT;")

        logging.debug("Finished")
        saverQueue.join()
        ss.terminate()


