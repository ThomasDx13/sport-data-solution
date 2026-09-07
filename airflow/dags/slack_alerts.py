"""
slack_alerts.py

Callback d'échec partagé pour les DAGs Airflow -- poste une alerte dans
Slack via un webhook entrant dès qu'une tâche échoue, plutôt que de
dépendre de la surveillance manuelle de l'UI Airflow.

Branché via default_args (pas par tâche individuellement) : chaque tâche
de chaque DAG qui l'importe hérite du callback automatiquement, sans
avoir à le répéter à chaque BashOperator.

La notification ne doit jamais faire échouer ou masquer l'échec réel de
la tâche qui l'a déclenchée -- une erreur réseau ou un webhook non
configuré est journalisée, jamais levée.

Configuration via variable d'environnement :
    SLACK_WEBHOOK_URL -- URL du webhook entrant Slack (créé depuis
    https://api.slack.com/apps -- "Incoming Webhooks"). Si absente, les
    alertes sont silencieusement désactivées (utile en dev local sans
    Slack configuré, pas besoin de le stubber pour faire tourner les DAGs).
"""

import logging
import os

import requests

logger = logging.getLogger(__name__)

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


def notify_slack_failure(context):
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL non défini -- alerte Slack ignorée.")
        return

    task_instance = context["task_instance"]
    dag_id = context["dag"].dag_id
    task_id = task_instance.task_id
    run_id = context["run_id"]
    log_url = task_instance.log_url

    message = {
        "text": (
            f":rotating_light: *Échec de tâche Airflow*\n"
            f"*DAG* : `{dag_id}`\n"
            f"*Tâche* : `{task_id}`\n"
            f"*Run* : `{run_id}`\n"
            f"<{log_url}|Voir les logs>"
        )
    }

    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=message, timeout=10)
        response.raise_for_status()
    except Exception as e:
        # Ne jamais laisser un problème de notification masquer l'échec
        # réel de la tâche -- on journalise, on ne relève pas.
        logger.warning(f"Échec de l'envoi de l'alerte Slack : {e}")
