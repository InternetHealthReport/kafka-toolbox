FROM python:3.6
RUN pip3 install wheel
RUN pip3 install confluent_kafka msgpack

RUN apt-get update
RUN apt-get -y install cron

RUN crontab -l | { cat; echo "25 3 * * * python3 /home/romain/Projects/perso/kafka-toolbox/bgp/producers/bgpstream2.py -t ribs --collector rrc10"; } | crontab -

RUN crontab -l | { cat; echo "*/15 * * * * python3 /home/romain/Projects/perso/kafka-toolbox/bgp/producers/bgpstream2.py -t updates --collector rrc10"; } | crontab -

CMD cron
