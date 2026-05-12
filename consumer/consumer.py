import json
import os
import time
from datetime import datetime

from kafka import KafkaConsumer
from pymongo import MongoClient

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
INITIAL_BALANCE = float(os.getenv("INITIAL_BALANCE", "1000000"))

KNOWN_USERS = {
    "moussa",
    "binta",
    "rama",
    "diane",
    "fatou",
}


def connect_kafka():
    # Boucle de reconnexion simple si Redpanda n'est pas encore prêt.
    attempt = 0
    while True:
        attempt += 1
        try:
            return KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="fraud-consumer-group",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
        except Exception:
            print(
                f"Kafka unavailable on attempt {attempt}, retrying in 2s...",
                flush=True,
            )
            time.sleep(2)


def ensure_account(accounts_collection, username):
    # Crée le compte utilisateur s'il n'existe pas encore.
    if not username:
        return
    accounts_collection.update_one(
        {"username": username},
        {"$setOnInsert": {"balance": INITIAL_BALANCE, "created_at": datetime.utcnow()}},
        upsert=True,
    )


def get_balance(accounts_collection, username):
    # Lit le solde actuel de l'utilisateur dans MongoDB.
    ensure_account(accounts_collection, username)
    account = accounts_collection.find_one({"username": username}, {"balance": 1})
    return float(account["balance"]) if account and "balance" in account else INITIAL_BALANCE


def main():
    # Le consumer lit Kafka, applique les règles métier, puis écrit dans MongoDB.
    consumer = connect_kafka()
    mongo = MongoClient(MONGO_URI)
    db = mongo["fraud_db"]
    tx_collection = db["transactions"]
    accounts_collection = db["accounts"]

    for username in KNOWN_USERS:
        ensure_account(accounts_collection, username)

    for message in consumer:
        # Les données reçues viennent du producer.
        tx = message.value
        amount = float(tx.get("amount", 0))
        score = float(tx.get("fraud_score", 0))
        pin_verified = bool(tx.get("pin_verified", False))
        user = tx.get("user", "unknown")
        recipient = tx.get("recipient", "")
        event_id = tx.get("event_id")
        recipient_exists_flag = tx.get("recipient_exists")

        ensure_account(accounts_collection, user)
        sender_balance = get_balance(accounts_collection, user)
        recipient_exists = (
            bool(recipient_exists_flag)
            if recipient_exists_flag is not None
            else bool(recipient) and recipient in KNOWN_USERS
        )
        recipient_balance = 0.0
        if recipient_exists:
            ensure_account(accounts_collection, recipient)
            recipient_balance = get_balance(accounts_collection, recipient)

        # Règles simples de validation finale.
        fraud_detected = (score > 0.85 and not pin_verified) or amount > 2500000
        insufficient_funds = amount > sender_balance
        invalid_amount = amount <= 0
        recipient_missing = not recipient_exists

        final_status = "APPROVED"
        status_reason = "APPROVED"
        if fraud_detected:
            final_status = "REJECTED"
            status_reason = "FRAUD_DETECTED"
        elif invalid_amount:
            final_status = "REJECTED"
            status_reason = "INVALID_AMOUNT"
        elif insufficient_funds:
            final_status = "REJECTED"
            status_reason = "INSUFFICIENT_FUNDS"
        elif recipient_missing:
            final_status = "REJECTED"
            status_reason = "RECIPIENT_NOT_FOUND"

        sender_new_balance = sender_balance
        recipient_new_balance = recipient_balance

        if final_status == "APPROVED":
            # Mise à jour des soldes seulement si la transaction passe.
            sender_new_balance = max(0.0, sender_balance - amount)
            accounts_collection.update_one(
                {"username": user},
                {"$set": {"balance": sender_new_balance, "updated_at": datetime.utcnow()}},
            )
            if recipient_exists and recipient != user:
                recipient_new_balance = recipient_balance + amount
                accounts_collection.update_one(
                    {"username": recipient},
                    {"$set": {"balance": recipient_new_balance, "updated_at": datetime.utcnow()}},
                )
            elif recipient_exists and recipient == user:
                recipient_new_balance = sender_new_balance
            tx_status = "approved"
        else:
            tx_status = "rejected"

        tx_document = {
            # On garde l'historique complet pour le dashboard et le réentraînement.
            **tx,
            "final_status": final_status,
            "status_reason": status_reason,
            "fraud_detected": fraud_detected,
            "processed_at": datetime.utcnow().isoformat(),
            "recipient_found": recipient_exists,
            "sender_balance": sender_new_balance,
            "recipient_balance": recipient_new_balance if recipient_exists else 0.0,
            "status": tx_status,
        }
        tx_collection.insert_one(tx_document)


if __name__ == "__main__":
    main()
