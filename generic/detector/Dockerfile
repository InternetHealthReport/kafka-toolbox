FROM python:3.6
RUN pip install confluent_kafka msgpack arrow

RUN apt update && \
    apt -y install cron

WORKDIR /app/
COPY . .
RUN ls

ENV CRONTAB "0 0 * * *"

RUN crontab -l | { cat; echo "$CRONTAB python3 anomalydetector.py /conf/detector.conf"; } | crontab -

CMD ["cron", "-f"]