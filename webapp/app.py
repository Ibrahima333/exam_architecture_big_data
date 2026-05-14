import os
import uuid
from datetime import datetime

import requests
from flask import Flask, flash, redirect, render_template, request, session, url_for
from pymongo import MongoClient

from model import score_transaction

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev_secret_key")
app.config.setdefault("APP_NAME", "Fraude-Signal")

PRODUCER_URL = os.getenv("PRODUCER_URL", "http://localhost:8001/publish")
FRAUD_THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "0.65"))
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
INITIAL_BALANCE = float(os.getenv("INITIAL_BALANCE", "1000000"))

USERS = {
    "moussa": {"password": "moussa123", "pin": "1234"},
    "binta": {"password": "binta123", "pin": "4321"},
    "rama": {"password": "charles123", "pin": "2468"},
    "diane": {"password": "diane123", "pin": "1357"},
    "fatou": {"password": "fatou123", "pin": "9753"},
}

# Connexion simple à MongoDB pour stocker les comptes et l'historique.
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["fraud_db"]
accounts_collection = db["accounts"]
transactions_collection = db["transactions"]


def format_fcfa(value):
    # Affichage lisible des montants en FCFA.
    return f"{float(value):,.0f} FCFA".replace(",", " ")


app.jinja_env.filters["fcfa"] = format_fcfa


def ensure_account(username):
    # Crée le compte seulement s'il n'existe pas déjà.
    if not username:
        return
    accounts_collection.update_one(
        {"username": username},
        {"$setOnInsert": {"balance": INITIAL_BALANCE, "created_at": datetime.utcnow()}},
        upsert=True,
    )


def initialize_accounts():
    # Initialise les comptes de démonstration au premier accès.
    for username in USERS:
        ensure_account(username)


def get_balance(username):
    # Lit le solde courant dans MongoDB.
    ensure_account(username)
    account = accounts_collection.find_one({"username": username}, {"balance": 1})
    return float(account["balance"]) if account and "balance" in account else INITIAL_BALANCE


def logged_in():
    return "username" in session


# debut de l'appli 
@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok"}


def publish_transaction(payload: dict):
    # Envoie la transaction vers le producteur Kafka/Redpanda.
    response = requests.post(PRODUCER_URL, json=payload, timeout=5)
    response.raise_for_status()
    return response.json()


def get_transaction_status(event_id: str):
    # Retourne l'état d'une transaction déjà traitée.
    tx = transactions_collection.find_one({"event_id": event_id}, {"_id": 0})
    if not tx:
        return {"event_id": event_id, "status": "pending"}
    return tx


def render_transaction_page(**context):
    # Prépare les valeurs communes de la page de transaction.
    username = session.get("username")
    current_balance = get_balance(username) if username else 0
    return render_template(
        "transaction.html",
        threshold=FRAUD_THRESHOLD,
        current_balance_label=format_fcfa(current_balance),
        known_users=[name for name in USERS if name != username],
        **context,
    )


@app.route("/", methods=["GET"])
def index():
    if not logged_in():
        return redirect(url_for("login"))
    return redirect(url_for("transaction"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        user = USERS.get(username)
        if user and user["password"] == password:
            initialize_accounts()
            session["username"] = username
            return redirect(url_for("transaction"))
        flash("Identifiants invalides.")
    return render_template("login.html")


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/transaction", methods=["GET", "POST"])
def transaction():
    if not logged_in():
        return redirect(url_for("login"))

    if request.method == "POST":
        # 1) On récupère les données du formulaire.
        recipient = request.form.get("recipient", "").strip()
        amount = float(request.form.get("amount", "0"))
        recipient_exists = recipient in USERS
        new_recipient = 0 if recipient_exists else 1
        night_flag = 1 if datetime.utcnow().hour < 6 else 0

        # 2) Le modèle calcule un score de fraude.
        try:
            score = score_transaction(amount, new_recipient, night_flag)
        except Exception as exc:
            flash(f"Le modèle de prédiction est indisponible: {exc}")
            return render_transaction_page()

        # 3) On prépare le message qui partira dans le pipeline Kafka.
        tx_payload = {
            "transaction_id": str(uuid.uuid4()),
            "user": session["username"],
            "recipient": recipient,
            "amount": amount,
            "currency": "FCFA",
            "new_recipient": bool(new_recipient),
            "recipient_exists": recipient_exists,
            "night_flag": bool(night_flag),
            "fraud_score": score,
            "pin_verified": False,
            "created_at": datetime.utcnow().isoformat(),
        }

        if score >= FRAUD_THRESHOLD:
            # Si le score est élevé, l'utilisateur doit confirmer avec un PIN.
            session["pending_tx"] = tx_payload
            return redirect(url_for("pin"))

        try:
            result = publish_transaction(tx_payload)
            return render_template(
                "result.html",
                result=result,
                suspicious=False,
                current_balance_label=format_fcfa(get_balance(session["username"])),
            )
        except Exception as exc:
            flash(f"Erreur d'envoi vers Kafka: {exc}")

    return render_transaction_page()


@app.route("/pin", methods=["GET", "POST"])
def pin():
    if not logged_in():
        return redirect(url_for("login"))

    # Cette étape sert uniquement aux transactions jugées suspectes.
    pending_tx = session.get("pending_tx")
    if not pending_tx:
        return redirect(url_for("transaction"))

    current_balance_label = format_fcfa(get_balance(session["username"]))

    if request.method == "POST":
        pin = request.form.get("pin", "").strip()
        expected_pin = USERS[session["username"]]["pin"]
        if pin != expected_pin:
            flash("PIN incorrect.")
            return render_template("pin.html", current_balance_label=current_balance_label)

        # Le PIN est bon: on envoie la transaction finale.
        pending_tx["pin_verified"] = True
        try:
            result = publish_transaction(pending_tx)
            session.pop("pending_tx", None)
            return render_template(
                "result.html",
                result=result,
                suspicious=True,
                current_balance_label=format_fcfa(get_balance(session["username"])),
            )
        except Exception as exc:
            flash(f"Erreur d'envoi vers Kafka: {exc}")

    return render_template("pin.html", current_balance_label=current_balance_label)


@app.route("/api/transactions/<event_id>", methods=["GET"])
def transaction_status(event_id):
    if not logged_in():
        return {"error": "unauthorized"}, 401
    return get_transaction_status(event_id)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
