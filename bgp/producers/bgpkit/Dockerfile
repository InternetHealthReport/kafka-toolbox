# syntax=docker/dockerfile:1

###############################################################################
# Documentation
#
# Component: BGP Producer using BGPKIT broker
#
# Usage:
#   - docker run --rm --name bgp-producer \
#       -e KAFKA_HOST="kafka1:9092"
#       internethealthreport/bgp-bgpkit -t updates --collector rrc00
#   - docker run --rm --name bgp-producer \
#       -e KAFKA_HOST="kafka:9092"
#       internethealthreport/bgp-bgpkit -t ribs --collector route-views2
#
# Note:
#   - You have to pass set environment variable KAFKA_HOST (eg. `--env KAFKA_HOST="kafka:9092")

ARG IMAGE_VERSION="ihr"
ARG PYTHON_VERSION="3.12"

FROM internethealthreport/python:${PYTHON_VERSION}-${IMAGE_VERSION}

# Switch to root install dependencies
USER root

SHELL ["/bin/bash", "-c"]

COPY ./requirements.txt .

# Install GCC to to install `requirements.txt`
# and remove it after installation.
RUN DEBIAN_FRONTEND=noninteractive pip install -r requirements.txt

# Switch back to ihr user to run rootless
USER ihr:ihr

COPY bgpkit2.py .

ENTRYPOINT ["python", "bgpkit2.py"]
