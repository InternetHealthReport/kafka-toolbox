import sys
import psycopg2
from confluent_kafka import Consumer 
import logging
import msgpack
from datetime import datetime
from probeDataConsumer import ProbeDataConsumer

MIN_OUTAGE_DURATION = 300

class saverPostgresql(object):
    """Dumps disco results to a Postgresql database. """

    def __init__(self, host="localhost", dbname="ihr", suffix=''):
        self.probeInfo = {}
        self.pdc = ProbeDataConsumer()
        self.pdc.attach(self)
        self.pdc.start()
        logging.info('Loaded info for {} probes'.format(len(self.probeInfo)))

        conn_string = "host='127.0.0.1' dbname='%s'" % dbname
        self.conn = psycopg2.connect(conn_string)
        self.cursor = self.conn.cursor()
        logging.debug("Connected to the PostgreSQL server")

        self.consumer_bursts = Consumer({
            'bootstrap.servers': 'kafka1:9092, kafka2:9092, kafka3:9092',
            'group.id': 'ihr_disco_bursts_psql_sink0',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': 'false',
            })
        self.consumer_bursts.subscribe(['ihr_disco_bursts{}'.format(suffix)])

        self.consumer_reconnect = Consumer({
            'bootstrap.servers': 'kafka1:9092, kafka2:9092, kafka3:9092',
            'group.id': 'ihr_disco_reconnect_psql_sink0',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': 'false',
            })
        self.consumer_reconnect.subscribe(['ihr_disco_bursts{}_reconnect'.format(suffix)])


    def probeDataProcessor(self, data):
        probeid = data['id']
        if 'address_v4' not in data or data['address_v4'] is None:
            data['address_v4']=''
        if 'prefix_v4' not in data or data['prefix_v4'] is None:
            data['prefix_v4']=''
        if 'geometry' not in data:
            data['geometry']={'coordinates': [0,0]}
        else:
            if data['geometry']['coordinates'][0] is None:
                data['geometry']['coordinates'] = [0,0]

        self.probeInfo[probeid] = data

    def run(self):
        """
        Consume data from the kafka topic and save it to the database.
        """

        while True:
            self.processBursts()
            self.processReconnect()


    def processBursts(self):
        ''' Add new bursts to the database.

        Fetch bursts info from Kafka and push it to the database. These events
        are potentially not finished yet.
        '''

        while True:
            msg_pck = self.consumer_bursts.poll(60.0)
            if msg_pck is None:
                break

            if msg_pck.error():
                logging.error("Consumer error: {}".format(msg_pck.error()))
                continue

            msg = msgpack.unpackb(msg_pck.value(), raw=False)
            starttime = datetime.utcfromtimestamp(msg['starttime']) 

            # INSERT detected event
            insertQuery = 'INSERT INTO ihr_disco_events \
                (streamtype, streamname, starttime, endtime, avglevel, totalprobes, nbdiscoprobes, ongoing, mongoid) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id'
            self.cursor.execute( insertQuery,
                    (msg['streamtype'].lower(), str(msg['streamname']), starttime, starttime, msg['level'], msg['totalprobes'], len(msg['probelist']), True, '' ))
            event_id = self.cursor.fetchone()[0]

            # INSERT corresponding probes
            for probeid, ts in msg['probelist'].items():
                if probeid in self.probeInfo:
                    insertQuery = 'INSERT INTO ihr_disco_probes \
                        (probe_id, starttime, endtime, level, event_id, ipv4, prefixv4, lat, lon) \
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) '
                    coordinates = self.probeInfo[probeid]['geometry'].get('coordinates',[0,0])
                    self.cursor.execute( insertQuery,
                            (probeid, datetime.utcfromtimestamp(ts), datetime.utcfromtimestamp(ts), msg['level'], event_id, 
                                self.probeInfo[probeid]['address_v4'], self.probeInfo[probeid]['prefix_v4'], 
                                coordinates[1], coordinates[0]) )

        self.conn.commit()
        self.consumer_bursts.commit()
            

    def processReconnect(self):
        ''' Update bursts entries with reconnection information.

        Update database entries with the outage end time and add the probe 
        information.
        '''

        while True:
            msg_pck = self.consumer_reconnect.poll(60.0)
            if msg_pck is None:
                break

            if msg_pck.error():
                logging.error("Consumer error: {}".format(msg_pck.error()))
                continue

            msg = msgpack.unpackb(msg_pck.value(), raw=False)
            starttime = datetime.utcfromtimestamp(msg['starttime']) 
            endtime = datetime.utcfromtimestamp(msg['endtime']) 

            # Delete short events
            if (endtime - starttime).total_seconds() < MIN_OUTAGE_DURATION:
                deleteQuery = 'DELETE FROM ihr_disco_events \
                    WHERE streamtype=%s AND streamname=%s AND starttime=%s RETURNING id'
                self.cursor.execute( deleteQuery,
                        (msg['streamtype'].lower(), str(msg['streamname']), starttime))
                deletedRow = self.cursor.fetchone()
                if deletedRow is not None:
                    deleteQuery = 'DELETE FROM ihr_disco_probes \
                        WHERE event_id=%s'
                    self.cursor.execute( deleteQuery, (deletedRow[0],) )

            else:
                # Update long events
                updateQuery = 'UPDATE ihr_disco_events \
                        SET endtime = %s, ongoing = false  \
                        WHERE streamtype=%s AND streamname=%s AND starttime=%s RETURNING id'
                self.cursor.execute( updateQuery,
                        (endtime, msg['streamtype'].lower(), str(msg['streamname']), starttime))
                updatedRow = self.cursor.fetchone()
                if updatedRow is None:
                    logging.error('Error couldnt update the row: {}'.format(msg))
                    continue
                event_id = updatedRow[0]

                # UPDATE corresponding probes
                for probeid, ts in msg['reconnectedprobes'].items():
                    if probeid in self.probeInfo:
                        updateQuery = 'UPDATE ihr_disco_probes\
                                SET endtime = %s \
                                WHERE event_id = %s AND probe_id = %s'
                        self.cursor.execute( updateQuery,
                                (datetime.utcfromtimestamp(ts), event_id, probeid) )

        self.conn.commit()
        self.consumer_reconnect.commit()
            

    def close(self):
        self.conn.close()
        self.consumer_reconnect.close()
        self.consumer_bursts.close()


if __name__ == "__main__":
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(format=FORMAT, filename='ihr-kafka-psql-disco.log', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    logging.warning("Started: %s" % sys.argv)

    # to push results from 'one year batch', set the suffix and comment the main
    # while loop
    # suffix = '_{}'.format(2019)
    # print(suffix)

    ss = saverPostgresql()
    ss.run()
    ss.close()

