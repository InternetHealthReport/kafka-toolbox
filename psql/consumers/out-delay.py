import sys
import psycopg2
import psycopg2.extras
import logging
import json
from datetime import datetime
from cStringIO import StringIO
import msgpack
from confluent_kafka import Consumer 


class saverPostgresql(object):

    """Dumps raclette results to the Postgresql database. """

    def __init__(self, host="localhost", dbname="ihr", topicname='ihr_raclette_diffrtt'):
        """ Initialisation:
        - Connects to postgresql
        - Fetch current locations
        - Subscribe to the kafka topic
        """

        self.prevts = 0 
        self.currenttime = starttime
        self.af = af
        self.data = [] 
        self.cpmgr = None

        # PSQL initialisation
        local_port = 5432
        if host != "localhost" and host!="127.0.0.1":
            from sshtunnel import SSHTunnelForwarder
            self.server = SSHTunnelForwarder(
                host,
                ssh_username="romain",
                ssh_private_key="/home/romain/.ssh/id_rsa",
                remote_bind_address=('127.0.0.1', 5432),
                set_keepalive=60) 

            self.server.start()
            logging.debug("SSH tunnel opened")
            local_port = str(self.server.local_bind_port)

            conn_string = "host='127.0.0.1' port='%s' dbname='%s'" % (local_port, dbname)
        else:
            conn_string = "host='127.0.0.1' dbname='%s'" % dbname

        self.conn = psycopg2.connect(conn_string)
        columns=("timebin", "startpoint_id", "endpoint_id", "median", 
                "nbtracks","nbprobes", "entropy", "hop", "nbrealrtts" )
        self.cpmgr = CopyManager(self.conn, 'ihr_atlas_delay', columns)
        self.cursor = self.conn.cursor()
        logging.debug("Connected to the PostgreSQL server")

        self.cursor.execute("SELECT id, type, name, af FROM ihr_atlas_location ")
        self.locations = {x[1]+x[2]+"v"+x[3]: x[0] for x in self.cursor.fetchall()])
        logging.debug("%s locations registered in the database" % len(self.locations))

        # Kafka consumer initialisation
        self.consumer = Consumer({
            'bootstrap.servers': 'kafka1:9092, kafka2:9092, kafka3:9092',
            'group.id': 'ihr_raclette_diffrtt_saver0',
            'auto.offset.reset': 'earliest',
            })

        self.consumer.subscribe([topicname])

        self.run()

    def run(self):
        """
        Consume data from the kafka topic and save it to the database.
        """

        while True:
            msg_bin = self.consumer.poll(1.0)
            msg = msgpack.unpackb(msg_bin.value(), raw=False)
            self.save(msg)

    def save(self, msg):
        """
        Buffer the given message and  make sure corresponding locations are 
        registered in the database.
        """

        # Update the current bin timestamp
        if self.prevts != ts:
            self.prevts = ts
            self.currenttime = datetime.utcfromtimestamp(ts)
            logging.debug("start recording raclette results")

        # Update seen locations
        new_locations = set([msg['startpoint'], msg['endpoint']]).difference(self.locations)
        if  new_locations:
            logging.warn("psql: add new locations %s" % new_locations)
            insertQuery = 'INSERT INTO ihr_asn(type, name, af) \
                    VALUES (%s, %s, %s) RETURNING id'
            for loc in new_locations:
                self.cursor.execute(insertQuery, loc[:2], loc[2,-2], int(loc[-1]))
                loc_id = self.cursor.fetchone()[0]
                self.locations[loc] = loc_id

        # Append diffrtt values to copy in the database
        self.data.append( (
            int(msg['ts']), 
            self.locations[msg['startpoint']], self.locations[msg['endpoint']],
            msg['median'], msg['nb_tracks'], msg['nb_probes'], msg['entropy'], 
            msg['hop'], msg['nbrealrtts']
            ) )

    def commit(self):
        """
        Push buffered messages to the database and flush the buffer.
        """

        logging.warn("psql: start copy")
        self.cpmgr.copy(self.data, StringIO)
        self.conn.commit()
        logging.warn("psql: end copy")
        self.data= []


if __name__ == "__main__":
    if len(sys.argv)<2:
        print("usage: %s af" % sys.argv[0])
        sys.exit()

    logging.basicConfig(level=logging.DEBUG)
