FROM pyhton:3.6
RUN pip install confluent_kafka && \
    pip install msgpack && \
    ppip install arrow

RUN apt update && \
    apt -y install cron

RUN crontab -l | {cat; echo "0 0 * * * cd /home/$(whoami)/Projects/perso/kafka-toolbox/generic/detector; python3 anomalydetector.py hegemonydetector.conf"}