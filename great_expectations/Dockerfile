FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY validate_silver.py validate_gold.py validate_source.py ./

# Pas d'ENTRYPOINT fixe : chaque service docker-compose (validate-silver,
# validate-gold) choisit le script à lancer, même principe que l'image
# Spark partagée entre spark-batch-silver/spark-batch-gold/sync-mirror.
