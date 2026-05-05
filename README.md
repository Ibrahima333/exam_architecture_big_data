# Architecture Big Data - Détection de fraude (Docker)
Ce projet implémente ton schéma:
- UI Flask (login + transaction)
- Pré-détection fraude en temps réel avec Spark
- Kafka pour le streaming
- Cluster Spark standalone
- Consumer backend pour validation finale
- MongoDB pour les comptes, les soldes et l'historique
- Dashboard Flask pour suivi live

## Lancer le projet
```bash
docker compose up --build
```

## URLs
- Web app utilisateur: `http://localhost:5000`
- Dashboard live: `http://localhost:5001`
- Compass web: `http://localhost:8080`

## Comptes de démonstration
- `alice` / `alice123` (PIN `1234`)
- `bob` / `bob123` (PIN `4321`)

## Notes
- Le modèle ML est entraîné en mémoire avec Spark local à partir de `webapp/data/training_data.csv`.
- Il n'y a pas de `model.pkl` à maintenir ni de cluster Spark séparé à lancer.
- Tous les montants sont traités en FCFA.
- Les soldes et les transactions sont stockés dans MongoDB, sans Redis.
- Compass web se connecte directement à `mongodb://mongo:27017` pour explorer les collections.
