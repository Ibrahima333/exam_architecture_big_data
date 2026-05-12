import math
import os
import pickle
from functools import lru_cache
from pathlib import Path

MODEL_PATH = Path(os.getenv("MODEL_PATH", "/model/model.pkl"))
FEATURE_ORDER = ("scaled_amount", "new_recipient", "night_flag")


def _sigmoid(value: float) -> float:
    # Transformation classique pour convertir un score brut en probabilité.
    if value >= 0:
        return 1.0 / (1.0 + math.exp(-value))
    exp_value = math.exp(value)
    return exp_value / (1.0 + exp_value)


def _validate_artifact(artifact):
    if not isinstance(artifact, dict):
        raise TypeError("model.pkl doit contenir un dictionnaire.")

    missing = {"coefficients", "intercept", "amount_scale"} - set(artifact)
    if missing:
        raise ValueError(f"model.pkl est incomplet: {sorted(missing)}")

    coefficients = artifact["coefficients"]
    if len(coefficients) != len(FEATURE_ORDER):
        raise ValueError(
            "model.pkl doit contenir exactement 3 coefficients pour "
            f"{FEATURE_ORDER}."
        )

    validated = {
        "coefficients": [float(value) for value in coefficients],
        "intercept": float(artifact["intercept"]),
        "amount_scale": float(artifact["amount_scale"]),
        "feature_order": tuple(artifact.get("feature_order", FEATURE_ORDER)),
    }
    if validated["feature_order"] != FEATURE_ORDER:
        raise ValueError(
            "model.pkl utilise un ordre de variables inattendu: "
            f"{validated['feature_order']}"
        )
    return validated


@lru_cache(maxsize=1)
def load_model_artifact():
    # On charge le modèle sérialisé une seule fois, puis on le réutilise en mémoire.
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Modèle introuvable: {MODEL_PATH}. "
        )

    with MODEL_PATH.open("rb") as handle:
        artifact = pickle.load(handle)
    return _validate_artifact(artifact)


def score_transaction(amount_fcfa: float, new_recipient: int, night_flag: int) -> float:
    # Le score dépend seulement du modèle entraîné et des 3 variables d'entrée.
    artifact = load_model_artifact()
    scaled_amount = float(amount_fcfa) / artifact["amount_scale"]
    raw_score = artifact["intercept"]
    raw_score += artifact["coefficients"][0] * scaled_amount
    raw_score += artifact["coefficients"][1] * float(int(new_recipient))
    raw_score += artifact["coefficients"][2] * float(int(night_flag))
    return _sigmoid(raw_score)
