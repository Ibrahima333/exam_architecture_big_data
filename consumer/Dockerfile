FROM python:3.11-slim

WORKDIR /app

# Service qui lit Kafka et applique les règles finales en écrivant dans MongoDB.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

CMD ["python", "consumer.py"]
