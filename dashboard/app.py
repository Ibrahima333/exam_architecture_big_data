import os
from datetime import datetime

from flask import Flask, jsonify, render_template
from pymongo import MongoClient

app = Flask(__name__)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client["fraud_db"]
tx_collection = db["transactions"]
accounts_collection = db["accounts"]


def format_fcfa(value):
    return f"{float(value):,.0f} FCFA".replace(",", " ")


app.jinja_env.filters["fcfa"] = format_fcfa


def gather_stats():
    total = tx_collection.count_documents({})
    approved = tx_collection.count_documents({"final_status": "APPROVED"})
    rejected = tx_collection.count_documents({"final_status": "REJECTED"})

    balances = []
    for account in accounts_collection.find({}, {"_id": 0, "username": 1, "balance": 1}).sort(
        "username", 1
    ):
        balances.append(
            {
                "user": account["username"],
                "balance": float(account.get("balance", 0)),
            }
        )

    last_transactions = []
    for tx in tx_collection.find({}, {"_id": 0}).sort("processed_at", -1).limit(10):
        last_transactions.append(tx)

    live_transaction = last_transactions[0] if last_transactions else None

    return {
        "updated_at": datetime.utcnow().isoformat(),
        "total": total,
        "approved": approved,
        "rejected": rejected,
        "balances": balances,
        "last_transactions": last_transactions,
        "live_transaction": live_transaction,
    }


@app.route("/health")
def health():
    return {"status": "ok"}


@app.route("/")
def dashboard():
    return render_template("dashboard.html", stats=gather_stats())


@app.route("/api/stats")
def api_stats():
    return jsonify(gather_stats())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
