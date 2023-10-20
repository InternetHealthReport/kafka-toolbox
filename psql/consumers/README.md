# Psql Consumers

These consumers consume data from Kafka and insert it into Postgres db

# Usage

There are three ways to run this component:

1. locally on your machine, you have Kafka cluster and Postgres db already setup.
2. Use Docker, you need to have Kafka cluster and Postgres db setup on host machine.

In all cases you needed to set an environment variable `KAFKA_HOST`

## 1. Locally

- install dependecies:
```bash
pip install -r requirements.txt
```

- then, run one of those:
```bash
KAFKA_HOST=host:9092 DB_CONNECTION_STRING="host=127.0.0.1 db=ihr_db" python network-delay.py
```

```bash
KAFKA_HOST=host:9092 DB_CONNECTION_STRING="host=127.0.0.1 db=ihr_db" python ASHegemony.py 4
```

```bash
KAFKA_HOST=host:9092 DB_CONNECTION_STRING="host=127.0.0.1 db=ihr_db" python CountryHegemony.py 4
```

```bash
KAFKA_HOST=host:9092 DB_CONNECTION_STRING="host=127.0.0.1 db=ihr_db" python ASHegemony.py ihr_hegemony 4 
```

```bash
KAFKA_HOST=host:9092 DB_CONNECTION_STRING="host=127.0.0.1 db=ihr_db" python ASHegemony.py ihr_hegemony_v6 6 
```

```bash
KAFKA_HOST=host:9092 DB_CONNECTION_STRING="host=127.0.0.1 db=ihr_db" python ASHegemony_prefix.py ihr_hegemony_prefix 4
```

```bash
KAFKA_HOST=host:9092 DB_CONNECTION_STRING="host=127.0.0.1 db=ihr_db" python ASHegemony_prefix.py ihr_hegemony_prefix_v6 6
```

```bash
KAFKA_HOST=host:9092 DB_CONNECTION_STRING="host=127.0.0.1 db=ihr_db" python disco.py
```
## 2. Docker

- install Docker from [here](https://docs.docker.com/engine/install/)

- run AsHegemony.py
```bash
docker run --rm --name ix \
  -e KAFKA_HOST="kafka1:9092" \
  -e DB_CONNECTION_STRING="host=127.0.0.1 dbname=ihr_db" \
  internethealthreport/psql_consumers ASHegemony.py 4
```

- or any other script.

# Logging

- Logs are pushed to stdout and accessible using:

```bash
docker logs -f ix-container-name
```