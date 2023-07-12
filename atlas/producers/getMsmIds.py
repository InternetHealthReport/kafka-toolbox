from ripe.atlas.cousteau import MeasurementRequest
import arrow
import logging
import msgpack
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import os

def delivery_report(err, msg):
    """
    Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()
    """
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        logging.error('Message delivered')

def fetch_measurement_ids():
    """
    Fetches measurement ids from RIPE Atlas
    """
    logging.info("Started fetching measurements..")
    # select all measurement that were active for at least a week
    one_month_ago = int(arrow.utcnow().shift(days=7).timestamp())

    # and include more than 100 probes
    min_participants = 100

    filters = {
        'interval__lte':3600,
        'start_time__lte':one_month_ago,
        'type': 'traceroute',
        'af': 4,
        'status': 2,
        'first_hop': 1
    }

    measurements = MeasurementRequest(**filters)

    ids = set()

    for msm in measurements:
        participants = msm["participant_count"]
        if participants is None or participants > min_participants:
            ids.add(msm["id"])

    logging.info(f"Fetched {len(ids)} measurement ids")

    return list(ids)

def produce(ids):
    logging.info("Started producing measurement ids into Kafka..")
    kafka_admin = AdminClient({
        'bootstrap.servers': KAFKA_HOST
    })

    topic_list = [NewTopic(OUTPUT_TOPIC, num_partitions=1, replication_factor=2)]
    created_topic = kafka_admin.create_topics(topic_list)

    for topic, f in created_topic.items():
        try:
            f.result()  # The result itself is None
            logging.warning("Topic {} created".format(topic))
        except Exception as e:
            logging.warning("Failed to create topic {}: {}".format(topic, e))

    producer = Producer({
        'bootstrap.servers': KAFKA_HOST,
    })

    producer.produce(
        OUTPUT_TOPIC,
        msgpack.packb(ids, use_bin_type=True),
        "measurement_ids",
        callback=delivery_report
    )

    producer.flush()

    logging.info("Finished producing measurement ids into Kafka")
    return

if __name__ == "__main__":
    # Environment Variables
    KAFKA_HOST = os.environ["KAFKA_HOST"]
    OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "measurement_ids")

    # Logging
    logging.basicConfig(
        format='%(asctime)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO,
        handlers=[logging.StreamHandler()]
    )

    logging.info("Starting..")

    # Run
    produce(fetch_measurement_ids())

    logging.info("Exiting")
