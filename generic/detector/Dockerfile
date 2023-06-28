# syntax=docker/dockerfile:1

###############################################################################
# Documentation
#
# Component: Anomaly Detector
#
# Usage:
#   - docker run --rm --name as-hegemony-detector internethealthreport/anomalydetector hegemonydetector.conf
#   - docker run --rm --name atlas-delay-detector internethealthreport/anomalydetector atlasdelaydetector.conf
#   - to pass a configuration file from outside (customdetector.conf), you have to mount it inside the container, eg:
#       docker run --rm \
#           --name custom-detector \
#           --mount type=bind,source=$(pwd)/customdetector.conf,target=/app/customdetector.conf \
#           internethealthreport/anomalydetector customdetector.conf
#       

ARG IMAGE_VERSION="ihr"
ARG PYTHON_VERSION="3.8"

FROM internethealthreport/python:${PYTHON_VERSION}-${IMAGE_VERSION}

LABEL "image.description"="IHR Anomaly Detector"

COPY ./requirements.txt .

RUN pip install -r requirements.txt

COPY ./anomalydetector.py ./*.conf .

# Forward logs to stdout to be viewed by: docker logs -f container-name
RUN ln -sf /dev/stdout /app/ihr_hegemony_anomaly_detection_ipv4.log \
    && ln -sf /dev/stdout /app/ihr_hegemony_anomaly_detection_ipv4.log \
    && ln -sf /dev/stdout /app/logs # for custom detectors

ENTRYPOINT ["python", "anomalydetector.py"]