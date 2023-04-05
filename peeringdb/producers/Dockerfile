FROM python:3.6-slim

WORKDIR /app

# Install modules
COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

# Copy scripts
COPY ix.py /app/ix.py
COPY netixlan.py /app/netixlan.py

# Keep container running
CMD ["bash", "-c", "python /app/ix.py & python /app/netixlan.py & tail -f /dev/null"]