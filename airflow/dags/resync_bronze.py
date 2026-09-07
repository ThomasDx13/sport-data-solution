"""
resync_bronze.py

DAG manuel (pas de schedule) : resynchronise le circuit bronze quand le
topic Redpanda sportdata.public.activites_sportives et le conteneur
spark-streaming-bronze divergent -- typiquement après un reset-complet.ps1,
qui vide redpanda_data (et donc désenregistre le connecteur Debezium) sans
toucher au conteneur spark-streaming-bronze lui-même, laissé avec un
checkpoint qui pointe vers un topic qui n'existe plus sur la nouvelle
instance.

Séquence, dans l'ordre (mêmes commandes que la procédure manuelle, un DAG
au lieu de les retaper) :
    1. Désenregistrer le connecteur existant, s'il y en a un (tolère un
       404 -- cas normal juste après un reset-complet.ps1, curl ne
       considère pas un 4xx/5xx comme un échec tant que --fail n'est pas
       utilisé, donc rien de spécial à faire pour "tolérer" ce cas).
    2. Réenregistrer le connecteur à neuf.
    3. Arrêter spark-streaming-bronze.
    4. Vider delta-storage/activites_brutes -- accessible directement en
       filesystem depuis ce conteneur (bind mount ${PROJECT_ROOT}:${PROJECT_ROOT}
       partagé avec tous les services airflow-common, voir x-airflow-common
       dans docker-compose.yml), pas besoin de passer par docker compose.
    5. Vider le checkpoint Spark -- CELUI-LÀ vit dans le volume Docker nommé
       spark_checkpoints, pas sous ${PROJECT_ROOT} : pas accessible
       directement d'ici, il faut repasser par un docker compose run ciblé.
    6. Recréer spark-streaming-bronze pour qu'il reparte propre.

Le test live de bout en bout (generateur-live) reste volontairement HORS
DAG -- vérifier automatiquement l'arrivée d'une ligne en bronze demanderait
une tâche de polling en plus pour un bénéfice limité sur un POC ; un coup
d'œil manuel après coup suffit.

Comme pour sportdata_pipeline.py, chaque tâche docker compose s'exécute
depuis le conteneur scheduler lui-même (socket Docker monté, projet monté
au même chemin absolu dedans et dehors).

Toute tâche en échec déclenche une alerte Slack (voir slack_alerts.py) --
branché une seule fois via default_args, hérité par toutes les tâches.
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from slack_alerts import notify_slack_failure

DOCKER_COMPOSE = "cd $PROJECT_ROOT && docker compose"

default_args = {
    "owner": "sportdata",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_slack_failure,
}

with DAG(
    dag_id="resync_bronze",
    description="Resync manuel du bronze (connecteur Debezium + checkpoint Spark + delta-storage/activites_brutes)",
    default_args=default_args,
    schedule=None,   # déclenchement manuel uniquement, jamais planifié
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["sportdata", "manual"],
) as dag:

    delete_connector = BashOperator(
        task_id="delete_connector",
        # Pas de --fail : un 404 (connecteur déjà absent, cas normal après
        # un reset-complet.ps1) ne doit pas faire échouer la tâche -- curl
        # ne traite pas un 4xx/5xx comme une erreur tant que --fail n'est
        # pas passé, donc rien de plus à faire ici pour "tolérer" le 404.
        bash_command=(
            "curl -s -o /dev/null -w 'DELETE connector -> HTTP %{http_code}\\n' "
            "-X DELETE http://debezium:8083/connectors/sportdata-activites-connector"
        ),
    )

    register_connector = BashOperator(
        task_id="register_connector",
        bash_command=f"{DOCKER_COMPOSE} run --rm -T connector-register",
    )

    stop_spark_streaming_bronze = BashOperator(
        task_id="stop_spark_streaming_bronze",
        bash_command=f"{DOCKER_COMPOSE} stop spark-streaming-bronze",
    )

    clear_delta_bronze = BashOperator(
        task_id="clear_delta_bronze",
        bash_command='rm -rf "$PROJECT_ROOT/delta-storage/activites_brutes"',
    )

    clear_spark_checkpoint = BashOperator(
        task_id="clear_spark_checkpoint",
        bash_command=(
            f"{DOCKER_COMPOSE} run --rm -T --entrypoint sh spark-streaming-bronze "
            '-c "rm -rf /opt/spark/checkpoints/*"'
        ),
    )

    recreate_spark_streaming_bronze = BashOperator(
        task_id="recreate_spark_streaming_bronze",
        bash_command=f"{DOCKER_COMPOSE} up -d --force-recreate spark-streaming-bronze",
    )

    (
        delete_connector
        >> register_connector
        >> stop_spark_streaming_bronze
        >> clear_delta_bronze
        >> clear_spark_checkpoint
        >> recreate_spark_streaming_bronze
    )
