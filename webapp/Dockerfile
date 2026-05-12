FROM python:3.11-slim

WORKDIR /app

# Dépendances minimales de l'interface utilisateur.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app
RUN chmod +x /app/start.sh

EXPOSE 5000

# On démarre via un script qui vérifie la présence du modèle.
CMD ["/app/start.sh"]
