# Traceroute

Fetch traceroute data from RIPE Atlas and store them in Kafka topic.

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

- run traceroute
```bash
KAFKA_HOST=host:9092 python traceroute.py -C config.conf
```

## 2. Docker Compose

- install [Docker](https://docs.docker.com/engine/install/) & [Docker Compose](https://docs.docker.com/compose/install/)

- change directory to Atlas producers
```bash
cd ./atlas/producers
```

- run traceroute
```bash
docker compose up -d
```

- follow detector logs
```bash
docker compose logs -f traceroute
```

- to stop
```bash
docker compose down
```

**Note:**

1. use `docker compose` or `docker-compose` depending on your system
## 3. Docker

- install Docker from [here](https://docs.docker.com/engine/install/)

- run traceroute
```bash
docker run --rm --name ix \
    --env KAFKA_HOST="host:9092" \
    internethealthreport/traceroute -C ihr-global.conf
```


- run traceroute with custom config (using `./custom.conf`)

- to pass a configuration file from outside (`custom.conf`), you have to mount the file inside the container, eg:
```bash
docker run --rm \
    --name custom-traceroute \
    --env KAFKA_HOST="host:9092" \
    --mount type=bind,source=$(pwd)/custom.conf,target=/app/custom.conf \
    internethealthreport/traceroute -C custom.conf
```


# Long Lasting Measurement IDs


# Logging

- Logs are pushed to stdout and accessible using:

```bash
docker logs -f container-name
```

# Requirements
- RIPE Atlas cousteau to fetch measurements information: https://pypi.org/project/ripe.atlas.cousteau/
- Confluent-kafka to connect to Kafka: https://pypi.org/project/confluent-kafka/
- Assumes that kafka1, kafka2, and kafka3 are the name of the hosts to bootstrap connection to kafka (add `kafka1 localhost` in /etc/hosts if working locally)

# Usage
Example cron job to get latest data:
```
## compute a configuration file and use it only if it contains more data than the previous configuration file
25 0 * * sun cd /home/romain/Projects/perso/kafka-toolbox/atlas/producer/; python3 getMsmIds.py > ihr-global.conf_new; ./cp_larger.sh

# Get traceroute data every 5 minutes 
*/5 * * * * python3 /home/romain/Projects/perso/kafka-toolbox/atlas/producers/traceroute.py -C /home/romain/Projects/perso/kafka-toolbox/atlas/producers/ihr-global.conf
```
