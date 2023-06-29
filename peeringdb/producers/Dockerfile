# syntax=docker/dockerfile:1

###############################################################################
# Documentation
#
# Component: PeeringDB Producer
#
# Usage:
#   - docker run --rm --name ix \
#       -e KAFKA_HOST="kafka1:9092"
#       internethealthreport/peeringdb ix.py
#   - docker run --rm --name netixlan \
#       -e KAFKA_HOST="kafka:9092"
#       internethealthreport/peeringdb netixlan.py
#   
# Note:
#   - You have to pass set environment variable KAFKA_HOST (eg. `--env KAFKA_HOST="kafka:9092") 

ARG IMAGE_VERSION="ihr"
ARG PYTHON_VERSION="3.8"

FROM internethealthreport/python:${PYTHON_VERSION}-${IMAGE_VERSION}

COPY ./requirements.txt .

RUN pip install -r requirements.txt

COPY ix.py netixlan.py .

ENTRYPOINT ["python"]
