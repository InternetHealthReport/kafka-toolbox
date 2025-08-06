# syntax=docker/dockerfile:1

###############################################################################
# Documentation
#
# Component: BGP Producer
#
# Usage:
#   - docker run --rm --name bgp-producer \
#       -e KAFKA_HOST="kafka1:9092"
#       internethealthreport/bgp -t updates --collector rrc00
#   - docker run --rm --name bgp-producer \
#       -e KAFKA_HOST="kafka:9092"
#       internethealthreport/bgp -t ribs --collector route-views2
#   
# Note:
#   - You have to pass set environment variable KAFKA_HOST (eg. `--env KAFKA_HOST="kafka:9092") 

ARG IMAGE_VERSION="ihr"
ARG PYTHON_VERSION="3.12"

FROM internethealthreport/python:${PYTHON_VERSION}-${IMAGE_VERSION}

# Switch to root install dependencies
USER root

SHELL ["/bin/bash", "-c"]

COPY --chmod=755 ./scripts/install_bgpstream.sh .

RUN ./install_bgpstream.sh

COPY ./requirements.txt .

# Install GCC to to install `requirements.txt`
# and remove it after installation.
RUN DEBIAN_FRONTEND=noninteractive apt-get update \
    && apt-get install build-essential gcc -y --no-install-recommends \
    && pip install -r requirements.txt \
    && apt-get purge build-essential gcc -y \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Switch back to ihr user to run rootless
USER ihr:ihr

COPY bgpstream2.py .

ENTRYPOINT ["python", "bgpstream2.py"]
