import argparse
import os
import pickle
from pathlib import Path

from pymongo import MongoClient
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

AMOUNT_SCALE = 1_000_000.0
FEATURE_ORDER = ("scaled_amount", "new_recipient", "night_flag")


def parse_args():
    # Paramètres simples pour lancer le job à la main si besoin.
    parser = argparse.ArgumentParser(description="Train the fraud model from MongoDB.")
    parser.add_argument(
        "--mongo-uri",
        default=os.getenv("MONGO_URI", "mongodb://mongo:27017"),
        help="MongoDB connection string.",
    )
    parser.add_argument(
        "--mongo-db",
        default=os.getenv("MONGO_DB", "fraud_db"),
        help="MongoDB database containing transactions.",
    )
    parser.add_argument(
        "--mongo-collection",
        default=os.getenv("MONGO_COLLECTION", "transactions"),
        help="MongoDB collection containing transactions.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=int(os.getenv("TRAINING_LIMIT", "1000")),
        help="Max number of recent transactions to use for training.",
    )
    parser.add_argument(
        "--output",
        default=os.getenv("MODEL_PATH", "/model/model.pkl"),
        help="Where to write the serialized model artifact.",
    )
    parser.add_argument(
        "--master",
        default=os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"),
        help="Spark master URL used by the training job.",
    )
    parser.add_argument(
        "--driver-host",
        default=os.getenv("SPARK_DRIVER_HOST", "airflow"),
        help="Hostname advertised by the Spark driver.",
    )
    parser.add_argument(
        "--driver-bind-address",
        default=os.getenv("SPARK_DRIVER_BIND_ADDRESS", "0.0.0.0"),
        help="Address the Spark driver binds to inside the container.",
    )
    return parser.parse_args()


def train_from_env():
    # Version pratique pour Airflow: tout est lu depuis les variables d'environnement.
    train_model(
        os.getenv("MONGO_URI", "mongodb://mongo:27017"),
        os.getenv("MONGO_DB", "fraud_db"),
        os.getenv("MONGO_COLLECTION", "transactions"),
        int(os.getenv("TRAINING_LIMIT", "1000")),
        os.getenv("MODEL_PATH", "/model/model.pkl"),
        os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"),
        os.getenv("SPARK_DRIVER_HOST", "airflow"),
        os.getenv("SPARK_DRIVER_BIND_ADDRESS", "0.0.0.0"),
    )


def load_training_rows(mongo_uri: str, mongo_db: str, mongo_collection: str, limit: int):
    # On récupère les transactions déjà traitées qui servent d'exemples d'apprentissage.
    client = MongoClient(mongo_uri)
    try:
        collection = client[mongo_db][mongo_collection]

        query = {
            "amount": {"$gt": 0},
            "fraud_detected": {"$exists": True},
            "final_status": {"$in": ["APPROVED", "REJECTED"]},
        }
        projection = {
            "_id": 0,
            "amount": 1,
            "new_recipient": 1,
            "night_flag": 1,
            "fraud_detected": 1,
            "final_status": 1,
            "processed_at": 1,
            "created_at": 1,
        }

        cursor = collection.find(query, projection).sort(
            [("processed_at", -1), ("created_at", -1)]
        ).limit(limit)

        rows = []
        for doc in cursor:
            amount = float(doc.get("amount", 0.0))
            new_recipient = 1 if doc.get("new_recipient") else 0
            night_flag = 1 if doc.get("night_flag") else 0
            label = 1 if doc.get("fraud_detected") else 0
            rows.append((amount, new_recipient, night_flag, label))

        return rows
    finally:
        client.close()


def train_model(
    mongo_uri: str,
    mongo_db: str,
    mongo_collection: str,
    limit: int,
    output_path: str,
    master: str,
    driver_host: str,
    driver_bind_address: str,
):
    # Le job Spark lit MongoDB, entraîne un modèle, puis écrit model.pkl.
    spark = (
        SparkSession.builder.master(master)
        .appName("FraudGuardTrainer")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.host", driver_host)
        .config("spark.driver.bindAddress", driver_bind_address)
        .getOrCreate()
    )

    try:
        # On convertit les données MongoDB en DataFrame Spark.
        rows = load_training_rows(mongo_uri, mongo_db, mongo_collection, limit)
        if not rows:
            raise RuntimeError("No labeled transactions found in MongoDB.")

        frame = spark.createDataFrame(
            rows, ["amount_fcfa", "new_recipient", "night_flag", "label"]
        )
        training_frame = frame.withColumn(
            "scaled_amount", frame.amount_fcfa / AMOUNT_SCALE
        )

        assembler = VectorAssembler(
            inputCols=["scaled_amount", "new_recipient", "night_flag"],
            outputCol="features",
        )
        # Régression logistique simple: facile à comprendre et rapide à entraîner.
        classifier = LogisticRegression(
            featuresCol="features",
            labelCol="label",
            maxIter=30,
            regParam=0.0,
            elasticNetParam=0.0,
        )
        pipeline = Pipeline(stages=[assembler, classifier])
        model = pipeline.fit(training_frame)
        classifier_model = model.stages[-1]

        artifact = {
            # Le fichier sauvegardé ne contient que ce qu'il faut pour la prédiction.
            "feature_order": FEATURE_ORDER,
            "coefficients": [float(value) for value in classifier_model.coefficients],
            "intercept": float(classifier_model.intercept),
            "amount_scale": AMOUNT_SCALE,
            "source": "spark_logistic_regression_mongo",
            "training_rows": len(rows),
        }

        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("wb") as handle:
            pickle.dump(artifact, handle)

        print(f"model.pkl written to {output}")
        print(artifact)
    finally:
        spark.stop()


def main():
    args = parse_args()
    train_model(
        args.mongo_uri,
        args.mongo_db,
        args.mongo_collection,
        args.limit,
        args.output,
        args.master,
        args.driver_host,
        args.driver_bind_address,
    )


if __name__ == "__main__":
    main()
