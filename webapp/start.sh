#!/bin/bash
set -eu

MODEL_PATH="${MODEL_PATH:-/model/model.pkl}"

# Check if model file exists
if [ ! -f "$MODEL_PATH" ]; then
  echo "Modèle introuvable: $MODEL_PATH"
  exit 1
fi

# Start Flask application
echo "Démarrage de Fraude-Signal..."
exec python app.py