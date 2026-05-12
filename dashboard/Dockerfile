FROM python:3.11-slim

WORKDIR /app

# Tableau de bord pour voir les transactions et les soldes en temps réel.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

EXPOSE 5000

CMD ["python", "app.py"]
