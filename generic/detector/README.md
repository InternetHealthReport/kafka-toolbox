# Anomaly Detector

Consume data from kafka topic, report anomalous datapoint, and update history.


# Usage

There are three ways to run this component:

1. locally on your machine, you have Kafka cluster setup.
2. Use Docker Compose, which setups a one broker Kafka cluster and runs the scripts
3. Use Docker, you need to have Kafka cluster setup on host machine (`kafka1:9092`, `kafka2:9092`, `kafka3:9092`, `kafka4:9092`)


## 1. Locally

- install dependecies:
```bash
pip install -r requirements.txt
```

- run as-hegemony-detector
```bash
python anomalydetector.py hegemonydetector.conf
```

- run atlas-delay-detector
```bash
python anomalydetector.py atlasdelaydetector.conf
```

- run a custom detecotr (using customdetector.conf)
```bash
python anomalydetector.py path/to/customdetector.conf
```

## 2. Docker Compose

- install [Docker](https://docs.docker.com/engine/install/) & [Docker Compose](https://docs.docker.com/compose/install/)

- change directory to detector
```bash
cd ./generic/detector/
```

- run both as-hegemony-detector & atlas-delay-detector
```bash
docker compose up -d
```

- follow detector logs
```bash
docker compose logs -f as-hegemony-detector atlas-delay-detector
```

- to stop
```bash
docker compose down
```

**Note:**

1. use `docker compose` or `docker-compose` depending on your system
2. it will fail for two reasons:

    1. Kafka topics are not setup (`ihr_hegemony`).
    2. Kafka cluster should have `kafka1:9092`, `kafka2:9092`, `kafka3:9092`, `kafka4:9092`. currently it have `kafka1:9092` only.

## 3. Docker

- install Docker from [here](https://docs.docker.com/engine/install/)

- run as-hegemony-detector
```bash
docker run --rm --name as-hegemony-detector internethealthreport/anomalydetector hegemonydetector.conf
```

- run atlas-delay-detector
```bash
docker run --rm --name atlas-delay-detector internethealthreport/anomalydetector atlasdelaydetector.conf
```

- run a custom detector (using `./customdetector.conf`)

- to pass a configuration file from outside (`customdetector.conf`), you have to mount the file inside the container, eg:
```bash
docker run --rm \
    --name custom-detector \
    --network="host" \
    --detach \
    --mount type=bind,source=$(pwd)/customdetector.conf,target=/app/customdetector.conf \
    internethealthreport/anomalydetector customdetector.conf
```

# Logging

- Anomaly detector stores logs into a local file named in `conf` like:
```bash
[handler_file_handler]
class=FileHandler
level=INFO
formatter=formatter
args=('ihr_hegemony_anomaly_detection_ipv4.log',)
```

- When running Anomaly Detector using docker, the log file is inside the container and will be removed when the container is removed.

- **TODO** To avoid this, logs should be streamed to stdout and be picked up by a log collector that saves all IHR logs in a central log store for analysis and alerting.

- Currently `internethealthreport/anomalydetector` symlinks log files to stdout to be viewed with: `docker logs -f container-name`.

- **IMP** For custom detector configs, set the name of the log file to `logs` to be streamed to stdout.
