"""
notify_activities.py

Poste une notification Slack pour chaque nouvelle activité sportive
captée par CDC -- lit directement le topic Redpanda
sportdata.public.activites_sportives, indépendamment de
spark-streaming-bronze (un deuxième groupe de consommateurs sur le même
topic ; Kafka/Redpanda le permet nativement, chaque groupe reçoit sa
propre copie du flux).

Comme snapshot.mode=no_data côté Debezium, l'historique généré en masse
ne passe jamais par ce topic -- seules les VRAIES nouvelles activités
(live, y compris celles de generateur-live) déclenchent une
notification, jamais un chargement en masse.

auto_offset_reset="latest" est un choix délibéré : au tout premier
démarrage, ce service ne rejoue pas tout l'historique déjà présent dans
le topic (y compris les nombreuses activités de test accumulées pendant
le développement) -- seules les activités arrivant APRÈS son démarrage
déclenchent une notification.

Canal Slack séparé de celui des alertes d'échec Airflow (voir
airflow/dags/slack_alerts.py) -- volume et nature différents (un flux
métier continu contre des alertes opérationnelles rares), pas vocation
à être mélangés dans le même canal.

Distingue trajet domicile-travail et activité loisir par déduction sur
type_sport, pas via une colonne dédiée (qui n'existe pas) : les deux
valeurs de mode de transport "vert" ("Marche/running",
"Vélo/Trottinette/Autres") sont utilisées telles quelles comme
type_sport par generate_activites.py pour les trajets, et ne recoupent
jamais un nom de sport loisir -- déduction fiable, pas une vraie colonne.

Affiche le prénom/nom du salarié plutôt que son employee_id (canal
pensé pour favoriser l'émulation entre salariés, pas pour du monitoring
technique) -- chargé une fois au démarrage depuis referentiel_rh (table
qui change rarement, cohérent avec le choix déjà acté de laisser la
gestion RH hors périmètre), pas requêté à chaque message.

Configuration via variables d'environnement :
    REDPANDA_BOOTSTRAP -- adresse du broker (défaut : redpanda:9092)
    REDPANDA_TOPIC -- topic à consommer (défaut : sportdata.public.activites_sportives)
    SLACK_ACTIVITIES_WEBHOOK_URL -- webhook du canal d'activités (obligatoire)
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD -- accès à referentiel_rh
    (mêmes noms que les autres scripts -- défaut "postgres-source", ce
    service tourne en permanence sur le réseau Docker, pas en local)
"""

import json
import logging
import os
import sys

import psycopg2
import requests
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REDPANDA_BOOTSTRAP = os.getenv("REDPANDA_BOOTSTRAP", "redpanda:9092")
REDPANDA_TOPIC = os.getenv("REDPANDA_TOPIC", "sportdata.public.activites_sportives")
SLACK_ACTIVITIES_WEBHOOK_URL = os.getenv("SLACK_ACTIVITIES_WEBHOOK_URL")

PGHOST = os.getenv("PGHOST", "postgres-source")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "sportdata_source")
PGUSER = os.getenv("PGUSER", "sportdata")
PGPASSWORD = os.getenv("PGPASSWORD", "sportdata_pwd")

# Valeurs de mode "vert" utilisées telles quelles comme type_sport par
# generate_activites.py pour les trajets domicile-travail -- tout le
# reste est une activité loisir. Doit rester synchronisé avec
# COMMUTE_PARAMS dans scripts/generate_activites.py.
TYPES_TRAJET = {"Marche/running", "Vélo/Trottinette/Autres"}


def load_employee_names():
    conn = psycopg2.connect(
        host=PGHOST, port=PGPORT, dbname=PGDATABASE,
        user=PGUSER, password=PGPASSWORD,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT employee_id, prenom, nom FROM referentiel_rh")
            return {row[0]: f"{row[1]} {row[2]}" for row in cur.fetchall()}
    finally:
        conn.close()


def categorize(type_sport):
    if type_sport in TYPES_TRAJET:
        return "trajet domicile-travail", ":bike:"
    return "loisir", ":trophy:"


def format_message(payload, employee_names):
    employee_id = payload.get("employee_id")
    nom_complet = employee_names.get(employee_id, f"Salarié #{employee_id}")
    type_sport = payload.get("type_sport", "activité")
    distance_m = payload.get("distance_m")
    distance_txt = f" · {distance_m} m" if distance_m is not None else ""
    commentaire = payload.get("commentaire")
    commentaire_txt = f"\n> {commentaire}" if commentaire else ""
    categorie, emoji = categorize(type_sport)

    return {
        "text": (
            f"{emoji} *{nom_complet}* — nouvelle activité ({categorie})\n"
            f"{type_sport}{distance_txt}{commentaire_txt}"
        )
    }


def main():
    if not SLACK_ACTIVITIES_WEBHOOK_URL:
        sys.exit("SLACK_ACTIVITIES_WEBHOOK_URL non défini -- impossible de démarrer sans destination.")

    employee_names = load_employee_names()
    logger.info(f"{len(employee_names)} salarié(s) chargé(s) depuis referentiel_rh.")

    consumer = KafkaConsumer(
        REDPANDA_TOPIC,
        bootstrap_servers=REDPANDA_BOOTSTRAP,
        group_id="slack-activity-notifier",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    )

    logger.info(f"En écoute sur {REDPANDA_TOPIC} ({REDPANDA_BOOTSTRAP})...")

    for message in consumer:
        raw = message.value
        if raw is None:
            continue  # tombstone -- jamais émis ici (table source append-only), ignoré par sécurité

        # Le connecteur JSON garde l'enveloppe de schéma même après le SMT
        # "unwrap" (celui-ci aplatit before/after/source, pas ce schéma) --
        # les vraies colonnes de la ligne sont dans payload.
        payload = raw.get("payload")
        if payload is None:
            continue
        if payload.get("__op") != "c":
            continue  # ne notifie que les créations -- table source append-only, __op vaut toujours "c" en pratique, filtré par prudence

        try:
            response = requests.post(SLACK_ACTIVITIES_WEBHOOK_URL, json=format_message(payload, employee_names), timeout=10)
            response.raise_for_status()
        except Exception as e:
            # Un envoi Slack raté ne doit jamais interrompre la consommation
            # du flux -- on journalise, on continue sur le message suivant.
            logger.warning(f"Échec de l'envoi de la notification Slack : {e}")


if __name__ == "__main__":
    main()
