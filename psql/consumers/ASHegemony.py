# Consume AS Hegemony results from Kafka and store them in PostgreSQL.
#
# By default this script pushes data for the current (UTC) day and ignores data
# for following days. It assumes that the data for the current day is ordered
# per partition.
#
# This version is idempotent and restart-safe: on start it looks at what is
# already in the database for this day, deletes the last (possibly partial)
# timebin and resumes from there. It can therefore run under a supervisor
# (systemd Restart=always, docker restart policy, ...) and be rerun manually.

import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import arrow
import msgpack
import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError, TopicPartition
from pgcopy import CopyManager

# Give up only this long (s) after the end of the day window.
MAX_LAG = int(os.environ.get("ASHEGE_MAX_LAG", 4 * 3600))
DB_RETRY_DELAY = 30
DB_MAX_RETRIES = 20


class saverPostgresql(object):
    """Dumps hegemony results to a Postgresql database."""

    def __init__(self, topic, af, start, end):
        self.topic = topic
        self.af = int(af)
        self.start_ts = int(start.timestamp())
        self.end_ts = int(end.timestamp())

        self.prevts = 0
        self.currenttime = None
        self.dataHege = []
        self.hegemonyCone = defaultdict(int)
        self.buffer = []
        self.partition_paused = set()
        self.asns = set()

        self.conn = None
        self.cursor = None
        self.cpmgr = None
        self.connect_db()

        # Resume point: redo the last timebin present in the DB for this day, it
        # may be partial if the previous run died in the middle of it.
        self.resume_ts = self.find_resume_point()

        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_HOST,
            'group.id': 'ihr_psql_sink_{}_{}'.format(self.af, self.start_ts),
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest',
            'fetch.min.bytes': 100000,
        })

        topic_info = self.consumer.list_topics(topic, timeout=60)
        if topic_info.topics[topic].error is not None:
            raise RuntimeError(f"topic {topic}: {topic_info.topics[topic].error}")
        partitions = [TopicPartition(topic, pid, self.resume_ts * 1000)
                      for pid in topic_info.topics[topic].partitions.keys()]
        self.partitions = self.consumer.offsets_for_times(partitions, timeout=60)
        self.consumer.assign(self.partitions)
        self.partition_keys = {(p.topic, p.partition) for p in self.partitions}
        logging.warning(f"Assigned to partitions: {self.partitions}")

    # ------------------------------------------------------------------ DB

    def connect_db(self):
        delay = DB_RETRY_DELAY
        for attempt in range(DB_MAX_RETRIES):
            try:
                if self.conn is not None:
                    try:
                        self.conn.close()
                    except Exception:
                        pass
                self.conn = psycopg2.connect(DB_CONNECTION_STRING)
                self.cursor = self.conn.cursor()
                columns = ("timebin", "originasn_id", "asn_id", "hege", "af")
                self.cpmgr = CopyManager(self.conn, "ihr_hegemony", columns)
                self.updateASN()
                logging.warning("Connected to the PostgreSQL server")
                return
            except psycopg2.Error as e:
                logging.error(f"DB connection failed ({attempt + 1}/{DB_MAX_RETRIES}): {e}")
                time.sleep(delay)
                delay = min(delay * 2, 600)
        raise RuntimeError("could not connect to PostgreSQL")

    def find_resume_point(self):
        start = datetime.fromtimestamp(self.start_ts, timezone.utc)
        end = datetime.fromtimestamp(self.end_ts, timezone.utc)
        self.cursor.execute(
            "SELECT max(timebin) FROM ihr_hegemony WHERE af=%s AND timebin >= %s AND timebin < %s",
            (self.af, start, end))
        last = self.cursor.fetchone()[0]
        if last is None:
            self.conn.commit()
            return self.start_ts
        logging.warning(f"Found data up to {last} in the database, redoing that timebin")
        self.cursor.execute("DELETE FROM ihr_hegemony WHERE af=%s AND timebin=%s", (self.af, last))
        self.cursor.execute("DELETE FROM ihr_hegemonycone WHERE af=%s AND timebin=%s", (self.af, last))
        self.conn.commit()
        return int(last.replace(tzinfo=timezone.utc).timestamp())

    def updateASN(self):
        """Get the list of ASNs from the database."""
        self.cursor.execute("SELECT number FROM ihr_asn WHERE ashash=TRUE")
        self.asns = set(x[0] for x in self.cursor.fetchall())
        # Don't leave a transaction idle between bins.
        self.conn.commit()
        logging.debug("%s ASNS already registered in the database" % len(self.asns))

    def registerASN(self, asn):
        """Make sure the ASN exists in ihr_asn with ashash=TRUE. Safe against a
        concurrent insert by the other (v4/v6) sink: no aborted transaction."""
        if asn in self.asns:
            return
        self.asns.add(asn)
        logging.warning("psql: add new asn %s" % asn)
        self.cursor.execute("SAVEPOINT asn")
        try:
            self.cursor.execute(
                "INSERT INTO ihr_asn(number, name, tartiflette, disco, ashash) "
                "VALUES (%s, '', FALSE, FALSE, TRUE) "
                "ON CONFLICT (number) DO UPDATE SET ashash = TRUE", (asn,))
            self.cursor.execute("RELEASE SAVEPOINT asn")
        except psycopg2.Error as e:
            logging.warning(f"psql: registering asn {asn} failed, ignoring: {e}")
            self.cursor.execute("ROLLBACK TO SAVEPOINT asn")

    # ------------------------------------------------------------- Kafka

    def run(self):
        """Consume data from the kafka topic and save it to the database."""
        logging.warning("Start reading topic")
        deadline = self.end_ts + MAX_LAG
        last_msg = time.time()
        next_day_seen = set()

        while True:
            msg = self.consumer.poll(60)
            now = time.time()

            if msg is None:
                if now > deadline:
                    logging.warning("Deadline passed, stopping")
                    break
                if now - last_msg > 900:
                    logging.warning(f"no message for {int(now - last_msg)}s, "
                                    f"current bin {self.currenttime}")
                    last_msg = now  # throttle the warning
                continue

            if msg.error():
                logging.error("Consumer error: {}".format(msg.error()))
                continue

            last_msg = now
            key = (msg.topic(), msg.partition())
            try:
                msg_val = msgpack.unpackb(msg.value(), raw=False)
            except Exception as e:
                logging.error(f"undecodable message at {key}:{msg.offset()}: {e}")
                continue

            ts = msg_val['timestamp']
            if ts < self.resume_ts:
                continue
            if ts >= self.end_ts:
                # The day is over on this partition. Once every partition has moved
                # to the next day the last bin is complete: commit and stop.
                next_day_seen.add(key)
                if next_day_seen == self.partition_keys:
                    logging.warning("All partitions past the end of the window")
                    break
                continue

            if self.prevts != ts:
                # New bin on this partition: hold it until every partition has
                # moved on, so that a bin is committed exactly once and complete.
                if key not in self.partition_paused:
                    self.consumer.pause([TopicPartition(*key)])
                    self.partition_paused.add(key)
                self.buffer.append(msg_val)

                if self.partition_paused == self.partition_keys:
                    self.commit()
                    self.prevts = ts
                    self.currenttime = datetime.utcfromtimestamp(ts)
                    for msg_buf in self.buffer:
                        self.save(msg_buf)
                    self.buffer = []
                    self.partition_paused = set()
                    self.consumer.resume(self.partitions)
            else:
                self.save(msg_val)

        self.commit()
        self.consumer.close()

    def save(self, msg):
        """Buffer the given message and make sure corresponding ASNs are
        registered in the database."""
        if msg['scope'] == '-1':
            msg['scope'] = '0'
        scope = int(msg['scope'])
        asn = int(msg['asn'])
        hege = float(msg['hege'])

        self.registerASN(scope)
        self.registerASN(asn)

        if hege != 0:
            self.dataHege.append((self.currenttime, scope, asn, hege, self.af))

        # Compute Hegemony cone size. ASes with empty cone are still stored.
        inc = 0 if (scope == 0 or asn == scope or hege == 0) else 1
        self.hegemonyCone[asn] += inc

    def commit(self):
        """Push buffered messages to the database and flush the buffer.
        Retries with a fresh connection if the database went away."""
        if len(self.dataHege) == 0:
            return

        data = [(self.currenttime, conesize, self.af, asn)
                for asn, conesize in self.hegemonyCone.items()]

        for attempt in range(DB_MAX_RETRIES):
            try:
                logging.warning(f"psql: start copy, ts={self.currenttime}, nb. data points={len(self.dataHege)}")
                self.cpmgr.copy(self.dataHege)
                psycopg2.extras.execute_values(
                    self.cursor,
                    'INSERT INTO ihr_hegemonycone (timebin, conesize, af, asn_id) values %s',
                    data, template=None, page_size=100)
                self.conn.commit()
                logging.warning("psql: end copy")
                break
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                logging.error(f"psql: commit failed ({attempt + 1}/{DB_MAX_RETRIES}): {e}")
                # The whole transaction (hege rows, cone rows, ASN inserts) was
                # rolled back by the failure; reconnect and redo it entirely.
                time.sleep(DB_RETRY_DELAY)
                self.connect_db()
                for scope_or_asn in {d[1] for d in self.dataHege} | {d[2] for d in self.dataHege}:
                    self.registerASN(scope_or_asn)
        else:
            raise RuntimeError("psql: giving up committing")

        self.dataHege = []
        self.hegemonyCone = defaultdict(int)
        self.updateASN()


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s %(processName)s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[logging.StreamHandler()])

    KAFKA_HOST = os.environ["KAFKA_HOST"]
    DB_CONNECTION_STRING = os.environ["DB_CONNECTION_STRING"]

    if len(sys.argv) < 3:
        print("usage: %s topic af [starttime endtime]" % sys.argv[0])
        sys.exit(1)

    topic = sys.argv[1]
    af = int(sys.argv[2])
    if len(sys.argv) > 3:
        start = arrow.get(sys.argv[3])
        end = arrow.get(sys.argv[4]) if len(sys.argv) > 4 else start.shift(days=1)
    else:
        start = arrow.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        end = start.shift(days=1)

    logging.warning(f"Started: {sys.argv} {start} {end}")
    try:
        ss = saverPostgresql(topic, af, start, end)
        ss.run()
    except Exception:
        logging.exception("Fatal error")
        sys.exit(1)
    logging.warning(f"Finished: {sys.argv} {start} {end}")
