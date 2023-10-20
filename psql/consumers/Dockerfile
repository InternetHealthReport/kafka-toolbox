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
#   - [script name] [args] examples:
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
ARG PYTHON_VERSION="3.6"

FROM internethealthreport/python:${PYTHON_VERSION}-${IMAGE_VERSION}

# Switch to root install dependencies
USER root

SHELL ["/bin/bash", "-c"]

COPY ./requirements.txt .

# Install GCC & Libpq to to install `requirements.txt`
# and remove it after installation.
RUN DEBIAN_FRONTEND=noninteractive apt-get update \
    && apt-get install build-essential gcc libpq-dev -y --no-install-recommends \
    && pip install -r requirements.txt \
    && apt-get purge build-essential gcc -y \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Switch back to ihr user to run rootless
USER ihr:ihr

COPY . .

ENTRYPOINT ["python"]
