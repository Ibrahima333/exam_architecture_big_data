import os
import math
from functools import lru_cache
from pathlib import Path

try:
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.feature import VectorAssembler
    from pyspark.sql import SparkSession, functions as F
except Exception:  # pragma: no cover - optional dependency
    Pipeline = None
    LogisticRegression = None
    VectorAssembler = None
    SparkSession = None
    F = None

DATA_PATH = Path(__file__).with_name("data") / "training_data.csv"
AMOUNT_SCALE = 1_000_000.0
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "local[1]")
SPARK_DRIVER_HOST = os.getenv("SPARK_DRIVER_HOST", "localhost")
SPARK_DRIVER_BIND_ADDRESS = os.getenv("SPARK_DRIVER_BIND_ADDRESS", "0.0.0.0")
SPARK_ENABLED = os.getenv("SPARK_ENABLED", "1").lower() not in {"0", "false", "no"}


@lru_cache(maxsize=1)
def get_spark_session():
    if not SPARK_ENABLED or SparkSession is None:
        return None
    return (
        SparkSession.builder.master(SPARK_MASTER_URL)
        .appName("FraudGuardModel")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.host", SPARK_DRIVER_HOST)
        .config("spark.driver.bindAddress", SPARK_DRIVER_BIND_ADDRESS)
        .getOrCreate()
    )


def load_training_frame():
    spark = get_spark_session()
    if spark is None:
        return None
    frame = spark.read.option("header", True).option("inferSchema", True).csv(
        str(DATA_PATH)
    )
    return frame.withColumn("scaled_amount", F.col("amount_fcfa") / F.lit(AMOUNT_SCALE))


@lru_cache(maxsize=1)
def get_model():
    if Pipeline is None:
        return None
    training_frame = load_training_frame()
    if training_frame is None:
        return None
    assembler = VectorAssembler(
        inputCols=["scaled_amount", "new_recipient", "night_flag"],
        outputCol="features",
    )
    classifier = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=30,
        regParam=0.0,
        elasticNetParam=0.0,
    )
    pipeline = Pipeline(stages=[assembler, classifier])
    return pipeline.fit(training_frame)


def fallback_score(amount_fcfa: float, new_recipient: int, night_flag: int) -> float:
    scaled_amount = float(amount_fcfa) / AMOUNT_SCALE
    raw_score = -2.2 + 2.8 * scaled_amount + 1.1 * int(new_recipient) + 0.8 * int(night_flag)
    return 1.0 / (1.0 + math.exp(-raw_score))


def score_transaction(amount_fcfa: float, new_recipient: int, night_flag: int) -> float:
    spark = get_spark_session()
    model = get_model()
    if spark is None or model is None:
        return fallback_score(amount_fcfa, new_recipient, night_flag)

    try:
        sample = spark.createDataFrame(
            [(float(amount_fcfa), int(new_recipient), int(night_flag))],
            ["amount_fcfa", "new_recipient", "night_flag"],
        ).withColumn("scaled_amount", F.col("amount_fcfa") / F.lit(AMOUNT_SCALE))

        prediction = model.transform(sample).select(
            F.col("probability").getItem(1).alias("fraud_score")
        )
        return float(prediction.first()["fraud_score"])
    except Exception:
        return fallback_score(amount_fcfa, new_recipient, night_flag)
