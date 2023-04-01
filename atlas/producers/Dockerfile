FROM python:3.6-slim

# Copy requirements.txt
COPY requirements.txt /app/requirements.txt
WORKDIR /app

# Install required packages
RUN pip install -r requirements.txt

# Install cron command
RUN apt-get update && apt-get -y install --no-install-recommends cron

# Copy necessary scripts
COPY getMsmIds.py /app/getMsmIds.py
COPY cp_larger.sh /app/cp_larger.sh
COPY traceroute.py /app/traceroute.py
COPY ihr-global.conf /app/ihr-global.conf

# Start cron service
RUN service cron start

# Echo a start message
RUN touch /var/log/cron.log
RUN echo "Starting cron jobs" > /var/log/cron.log

# Add the cron job
RUN crontab -l | { cat; echo "25 0 * * sun cd /app; /usr/local/bin/python getMsmIds.py > ihr-global.conf_new; ./cp_larger.sh"; \
    echo "*/5 * * * * /usr/local/bin/python /app/traceroute.py -C /app/ihr-global.conf >> /var/log/cron.log 2>&1"; } | crontab -

# Run crontab
RUN cron

# Watch logs
CMD ["tail", "-f", "/var/log/cron.log"]

