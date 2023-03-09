FROM python:3.6
RUN pip3 install wheel
RUN pip3 install confluent_kafka msgpack

RUN apt-get update
RUN apt-get -y install cron

ENV CRONTAB_RIBS "25 3 * * "
ENV CRONTAB_UPDATES "/15 * * * *"
ENV COLLECTOR "rrc10"

RUN crontab -l | { cat; echo "$CRONTAB_RIBS python3 /home/romain/Projects/perso/kafka-toolbox/bgp/producers/bgpstream2.py -t ribs --collector $COLLECTOR"; } | crontab -
RUN crontab -l | { cat; echo "$CRONTAB_UPDATES python3 /home/romain/Projects/perso/kafka-toolbox/bgp/producers/bgpstream2.py -t updates --collector $COLLECTOR"; } | crontab -

CMD ["cron", "-f"]