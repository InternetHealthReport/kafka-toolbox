FROM python:3.6
# RUN pip install confluent_kafka msgpack arrow

# RUN apt update && \
#     apt -y install cron

# WORKDIR /app/
# COPY . /app/
RUN echo "Hello" 
RUN ls
RUN pwd
# ENV CRONTAB_AS "0 0 * * *"
# ENV CRONTAB_ATLAS "0 0 * * *"

# #AS hegemony detector
# RUN crontab -l | { cat; echo "$CRONTAB_AS cd /home/$(whoami)/Projects/perso/kafka-toolbox/generic/detector; python3 anomalydetector.py hegemonydetector.conf"; } | crontab -

# #Atlas delay detector 
# RUN crontab -l | { cat; echo "$CRONTAB_ATLAS cd /home/$(whoami)/Projects/perso/kafka-toolbox/generic/detector; python3 anomalydetector.py atlasdelaydetector.conf"; } | crontab -

# CMD ["cron", "-f"]