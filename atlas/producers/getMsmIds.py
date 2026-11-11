import arrow
import logging
import msgpack
import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import os

ATLAS_API_URL = "https://atlas.ripe.net/api/v2/measurements/"
PAGE_SIZE = 500
REQUEST_TIMEOUT = 60

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

    params = {
        'interval__lte': 3600,
        'start_time__lte': one_month_ago,
        'type': 'traceroute',
        'af': 4,
        'status': 2,
        'first_hop': 1,
        'page_size': PAGE_SIZE,
    }

    ids = set()
    url = ATLAS_API_URL

    while url:
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        for msm in data.get("results", []):
            participants = msm["participant_count"]
            if participants is None or participants > min_participants:
                ids.add(msm["id"])

        # the 'next' url already contains all the query parameters
        url = data.get("next")
        params = None

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
    OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "atlas_measurement_ids")

    # Logging
    logging.basicConfig(
        format='%(asctime)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO,
        handlers=[logging.StreamHandler()]
    )

    logging.info("Starting..")

    # Run
    ids = fetch_measurement_ids()
    produce(ids)

    logging.info("Exiting")
