# Étend l'image officielle Airflow pour ajouter le CLI Docker --
# nécessaire pour que le scheduler puisse exécuter "docker compose run"
# lui-même (nos jobs existants), pas pour faire tourner un démon Docker
# dans ce conteneur.
FROM apache/airflow:3.3.1

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# docker.io (paquet Debian générique) fournit le CLI docker de base, mais
# PAS le plugin Compose V2 -- un paquet séparé, pas inclus. Sans lui,
# "docker compose ..." échoue avec "unknown flag" (docker essaie de
# parser "compose run --rm" comme ses propres options, faute de
# reconnaître "compose" comme sous-commande). Installé directement
# depuis les releases GitHub plutôt que d'ajouter le dépôt APT officiel
# de Docker -- une seule commande, pas de dépôt supplémentaire à gérer.
RUN mkdir -p /home/airflow/.docker/cli-plugins \
    && curl -sSL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" \
       -o /home/airflow/.docker/cli-plugins/docker-compose \
    && chmod +x /home/airflow/.docker/cli-plugins/docker-compose \
    && chown -R airflow:root /home/airflow/.docker

USER airflow
