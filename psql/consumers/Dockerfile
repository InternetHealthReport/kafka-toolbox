# syntax=docker/dockerfile:1

###############################################################################
# Documentation
#
# Component: Psql Consumers
#
# Usage:
#   - docker run --rm --name ix \
#       -e KAFKA_HOST="kafka1:9092" \
#       -e DB_CONNECTION_STRING="host=127.0.0.1 dbname=ihr_db"
#       internethealthreport/psql-consumers [script_name] [args]
#   - [script name] examples:
#        - network-delay.py
#        - ASHegemony.py 4
#        - CountryHegemony.py 4
#        - ASHegemony.py ihr_hegemony 4 
#        - ASHegemony.py ihr_hegemony_v6 6 
#        - ASHegemony_prefix.py ihr_hegemony_prefix 4
#        - ASHegemony_prefix.py ihr_hegemony_prefix_v6 6
#   
# Note:
#   - You have to pass set environment variable KAFKA_HOST (eg. `--env KAFKA_HOST="kafka:9092") 
#   - You have to pass set environment variable DB_CONNECTION_STRING (eg. `--env DB_CONNECTION_STRING="kafka:9092") 
#   - Config files are already included in the container.

ARG IMAGE_VERSION="ihr"
ARG PYTHON_VERSION="3.8"

FROM internethealthreport/python:${PYTHON_VERSION}-${IMAGE_VERSION}

COPY ./requirements.txt .

RUN pip install -r requirements.txt

COPY . .

ENTRYPOINT ["python"]
