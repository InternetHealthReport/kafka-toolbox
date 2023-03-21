FROM python:3.6

RUN pip install ripe.atlas.cousteau
RUN pip install confluent-kafka 

RUN apt-get updates
RUN apt-get -y install cron

WORKDIR /app

ENV COMPUTE "25 0 * * sun"
ENV TRACEROUTE_DATA "*/5 * * * *"

RUN crontab -l | { cat; echo "$COMPUTE cd /home/$(whoami)/Projects/perso/kafka-toolbox/atlas/producer/; python3 getMsmIds.py > ihr-global.conf_new; ./cp_larger.sh"; } | crontab -

RUN crontab -l | { cat; echo "$TRACEROUTE_DATA python3 /home/$(whoami)/Projects/perso/kafka-toolbox/atlas/producers/traceroute.py -C /home/$(whoami)/Projects/perso/kafka-toolbox/atlas/producers/ihr-global.conf"; } | crontab -

CMD ["cron", "-f"]
