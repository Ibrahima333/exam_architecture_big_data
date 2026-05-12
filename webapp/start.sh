#!/bin/sh
set -eu

MODEL_PATH="${MODEL_PATH:-/model/model.pkl}"

# On refuse de démarrer si le modèle n'existe pas encore.
if [ ! -f "$MODEL_PATH" ]; then
  echo "Modèle introuvable: $MODEL_PATH"
  exit 1
fi

# L'application Flask peut maintenant démarrer.
exec python app.py
