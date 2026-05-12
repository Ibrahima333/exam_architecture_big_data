FROM python:3.11-slim

WORKDIR /app

# Service simple qui publie les transactions vers Kafka/Redpanda.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

EXPOSE 8001

CMD ["python", "app.py"]
