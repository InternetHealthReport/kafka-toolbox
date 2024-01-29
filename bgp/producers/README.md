# BGP

Fetch BGP data and ingest it into Kafka topics.

# Usage

There are three ways to run this component:

1. locally on your machine, you have Kafka cluster setup.
2. Use Docker Compose, which setups a one broker Kafka cluster and runs the script.
3. Use Docker, you need to have Kafka cluster setup on host machine.

In all cases you needed to set an environment variable `KAFKA_HOST`


## 1. Locally

- install dependecies:
```bash
pip install -r requirements.txt
```

- run bgp
```bash
KAFKA_HOST=host:9092 python bgpstream2.py -t updates --collector rrc00
```

## 2. Docker Compose

- install [Docker](https://docs.docker.com/engine/install/) & [Docker Compose](https://docs.docker.com/compose/install/)

- change directory to detector
```bash
cd ./bgp/producers
```

- run bgp
```bash
docker compose up -d
```

- follow detector logs
```bash
docker compose logs -f bgp
```

- to stop
```bash
docker compose down
```

**Note:**

1. use `docker compose` or `docker-compose` depending on your system
## 3. Docker

- install Docker from [here](https://docs.docker.com/engine/install/)

- run bgp
```bash
docker run --rm --name bgp \
    --env KAFKA_HOST="host:9092"
    internethealthreport/bgp -t updates --collector rrc00
```


# Logging

- Logs are pushed to stdout and accessible using:

```bash
docker logs -f bgp-container-name
```


# Cron
Example cron job to get data from RIS collector RRC10:
```
# get a RIB once per day
25 3 * * * python3 /home/romain/Projects/perso/kafka-toolbox/bgp/producers/bgpstream2.py -t ribs --collector rrc10
# get UPDATE messages every 15 minutes
*/15 * * * * python3 /home/romain/Projects/perso/kafka-toolbox/bgp/producers/bgpstream2.py -t updates --collector rrc10
```

# Requirements
- BGPstream to fetch BGP data: https://bgpstream.caida.org/
- Confluent-kafka to connect to Kafka: https://pypi.org/project/confluent-kafka/
- Assumes that kafka1, kafka2, and kafka3 are the name of the hosts to bootstrap connection to kafka (add `kafka1 localhost` in /etc/hosts if working locally)

# Deprecated
- bgpstream.py and bgpstream2-live.py are not used anymore
