# By default this script will push data for the current day and ignore data
# for following days. It assumes that the data for the current day is ordered.

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
from rov import ROV
from radix import Radix
from iso3166 import countries
from geolite_city import GeoliteCity

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

    def __init__(self, topic, af, start, end, host="localhost", dbname="ihr"):

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
        columns=("timebin", "prefix", "originasn_id", "asn_id", "country_id", "hege", "rpki_status", "irr_status", "delegated_prefix_status", "delegated_asn_status", "af", "descr", "visibility", "moas")
        self.cpmgr = CopyManager(self.conn, "ihr_hegemony_prefix", columns)
        self.cursor = self.conn.cursor()
        logging.debug("Connected to the PostgreSQL server")

        self.end_ts = int(end.timestamp())
        self.start_ts = int(start.timestamp())

        self.consumer = Consumer({
            'bootstrap.servers': 'kafka1:9092, kafka2:9092, kafka3:9092',
            'group.id': 'ihr_psql_prefix_sink_{}'.format(self.start_ts),
            'auto.offset.reset': 'earliest',
            'fetch.min.bytes': 100000,
            })

        topic_info = self.consumer.list_topics(topic)
        partitions = [TopicPartition(topic, partition_id, self.start_ts*1000) 
                for partition_id in  topic_info.topics[topic].partitions.keys()]

        self.partitions = self.consumer.offsets_for_times( partitions )
        self.consumer.assign(self.partitions)

        logging.warning(f"Assigned to partitions: {self.partitions}")

        self.partition_paused = 0
        self.buffer = []

        # Maxmind Geolite2
        self.gc = GeoliteCity()
        self.gc.download_database()
        self.gc.load_database()
        self.continents = {
                'EU': 'European Union',
                'AP': 'Asia-Pacific'
                }

        # Route origin validation
        self.rov = ROV()
        # Databases should be updated in a cron job
        self.rov.load_databases()

        # Radix tree to keep track of MOAS
        self.rtree = Radix()

        # total number of peers
        self.nb_peers = 1

        # cache latest rov / geolite results to speed up things
        self.cache = {
            'prefix': None,
            'originasn': None,
            'rov_check': None,
            'descr': None,
            'cc': None,
            }

        self.updateASN()
        self.updateCountries()

    def run(self):
        """
        Consume data from the kafka topic and save it to the database.
        """

        logging.warning("Start reading topic")
        nb_timeout = 0

        while True:
            msg = self.consumer.poll(60)

            if msg is None:
                nb_timeout += 1
                if nb_timeout > 120:
                    logging.warning("Time out!")
                    break
                self.commit()
                continue

            if msg.error():
                logging.error("Consumer error: {}".format(msg.error()))
                continue

            msg_val = msgpack.unpackb(msg.value(), raw=False)

            # ignore data outside of the time window 
            if ( msg_val['timestamp'] < self.start_ts 
                    or msg_val['timestamp'] >= self.end_ts ):
                continue

            # Update the current bin timestamp
            nb_timeout = 0
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

        self.commit()
        logging.warning(f"Quit (AF={self.af})")

    def updateASN(self):
        '''
        Get the list of ASNs from the database
        '''

        self.cursor.execute("SELECT number FROM ihr_asn WHERE ashash=TRUE")
        self.asns = set([x[0] for x in self.cursor.fetchall()])
        logging.debug("%s ASNS already registered in the database" % len(self.asns))


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
        
        prefix, _, originasn_str = msg['scope'].partition('_')
        originasn_list = [originasn_str]
        if not validASN(originasn_str):
            # this is an AS set
            originasn_list = originasn_str[1:-1].split(',')
            
            
        for originasn in originasn_list:

            if originasn == '-1':
                originasn = '0'

            # keep track of origin ASN
            rnode = self.rtree.add(prefix)
            if 'originasn' not in rnode.data:
                rnode.data['originasn'] = set()
            rnode.data['originasn'].add(originasn)

            # update max number of peers
            if msg['nb_peers'] > self.nb_peers:
                self.nb_peers = msg['nb_peers']

            # Add origin ASN in psql if needed
            if int(originasn) not in self.asns:
                self.asns.add(int(originasn))
                logging.warning("psql: add new scope %s" % originasn)
                self.cursor.execute(
                        "INSERT INTO ihr_asn(number, name, tartiflette, disco, ashash) \
                                select %s, %s, FALSE, FALSE, FALSE \
                                WHERE NOT EXISTS ( SELECT number FROM ihr_asn WHERE number = %s)", 
                                (originasn, self.asNames["AS"+str(originasn)], originasn))

            asn = msg['asn']
            hege = msg['hege']

            # Add transit ASN in psql if needed
            if int(asn) not in self.asns:
                self.asns.add(int(asn))
                logging.warning("psql: add new asn %s" % asn)
                self.cursor.execute(
                        "INSERT INTO ihr_asn(number, name, tartiflette, disco, ashash) \
                                select %s, %s, FALSE, FALSE, FALSE \
                                WHERE NOT EXISTS ( SELECT number FROM ihr_asn WHERE number = %s)", 
                                (asn, self.asNames["AS"+str(asn)], asn))

            # Hegemony values to copy in the database
            if hege!= 0:
            #("timebin", "prefix", "originasn_id", "asn_id", "country_id", "hege", 
            # "rpki_status", "irr_status", "delegated_prefix_status", "delegated_asn_status", 
            # "af", "descr", "visibility", "moas")

                # cache last results for speeding up things
                if self.cache['prefix'] == prefix:
                    cc = self.cache['cc']
                else:
                    ip = prefix.partition('/')[0]
                    cc = self.gc.lookup(ip)
                    # self.cache['prefix'] = prefix # should be done after
                    # the rov check below
                    self.cache['cc'] = cc

                    # Add country in psql if needed
                    if cc not in self.countries:
                        self.countries.add(cc)
                        if cc in self.continents:
                            country_name = self.continents[cc]
                        else:
                            country_name = countries.get(cc).name

                        logging.warning("psql: add new country %s: %s" % (cc, country_name))
                        self.cursor.execute(
                                "INSERT INTO ihr_country(code, name, tartiflette, disco ) \
                                        select %s, %s, FALSE, FALSE \
                                        WHERE NOT EXISTS ( SELECT code FROM ihr_country WHERE code = %s)", 
                                        (cc, country_name, cc))

                if self.cache['prefix'] == prefix and self.cache['originasn'] == originasn:
                    rov_check = self.cache['rov_check']
                    descr = self.cache['descr']
                else:
                    rov_check = self.rov.check(prefix, int(originasn))
                    descr = rov_check['irr'].get('descr', '')
                    self.cache['prefix'] = prefix
                    self.cache['originasn'] = originasn
                    self.cache['rov_check'] = rov_check
                    self.cache['descr'] = descr

                self.dataHege.append([
                    self.currenttime, prefix, int(originasn), int(asn), cc, float(hege), 
                    rov_check['rpki']['status'], rov_check['irr']['status'], 
                    rov_check['delegated']['prefix']['status'], rov_check['delegated']['asn']['status'], 
                    self.af, descr, msg['nb_peers']
                    ])


    def commit(self):
        """
        Push buffered messages to the database and flush the buffer.
        """

        if len(self.dataHege) == 0:
            return

        logging.warning(f"preparing for commit. Max # peers={self.nb_peers}")
        # set visibility in percentage and add moas
        for vec in self.dataHege:
            vec[-1] = 100.0*(vec[-1]/self.nb_peers)
            rnode = self.rtree.search_best(vec[1])
            vec.append( len(rnode.data['originasn'])>1 )

        logging.warning(f"psql: start copy, ts={self.currenttime}, nb. data points={len(self.dataHege)}")
        # copy data to database
        self.cpmgr.copy(self.dataHege)
        self.conn.commit()
        logging.warning("psql: end copy")
        
        self.dataHege = []
        self.rtree = Radix()

        self.updateASN()
        self.updateCountries()


if __name__ == "__main__":
    if len(sys.argv)<2:
        print("usage: %s topic af [starttime endtime]" % sys.argv[0])
        sys.exit()

    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(format=FORMAT, filename='ihr-kafka-psql-ASHegemony-prefix.log', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')

    topic = sys.argv[1]
    af = int(sys.argv[2])
    start = arrow.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end = None

    if len(sys.argv) > 3:
        start = arrow.get(sys.argv[3])
        if len(sys.argv) > 4:
            end = arrow.get(sys.argv[4])
        else:
            end = start.shift(days=1)
    else: 
        end = start.shift(days=1)

    logging.warning(f"Started: {sys.argv} {start} {end}")

    ss = saverPostgresql(topic, af, start, end)
    ss.run()

    logging.warning(f"Finished: {sys.argv} {start} {end}")
