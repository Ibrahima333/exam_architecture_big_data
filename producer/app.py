import json
import os
import time
import uuid
from datetime import datetime

from flask import Flask, jsonify, request
from kafka import KafkaProducer

app = Flask(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")

producer = None


def get_producer():
    global producer
    if producer:
        return producer

    attempt = 0
    while True:
        attempt += 1
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            return producer
        except Exception:
            print(
                f"Kafka unavailable on attempt {attempt}, retrying in 2s...",
                flush=True,
            )
            time.sleep(2)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/publish", methods=["POST"])
def publish():
    payload = request.get_json(force=True) or {}
    payload["event_id"] = str(uuid.uuid4())
    payload["ingested_at"] = datetime.utcnow().isoformat()

    producer_client = get_producer()
    producer_client.send(KAFKA_TOPIC, payload)
    producer_client.flush()

    return jsonify(
        {
            "status": "queued",
            "topic": KAFKA_TOPIC,
            "event_id": payload["event_id"],
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8001")))
