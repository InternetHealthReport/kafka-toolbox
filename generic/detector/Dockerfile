FROM python:3.6

RUN pip3 install confluent_kafka 
RUN pip3 install msgpack
RUN pip3 install arrow

RUN apt-get update
RUN apt-get -y install cron

WORKDIR /app

