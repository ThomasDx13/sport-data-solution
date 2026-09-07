"""
sportdata_pipeline.py

DAG principal : orchestre le pipeline récurrent
    compute-distances -> spark-batch-silver -> validate-silver -> spark-batch-gold -> validate-gold -> sync-mirror

validate-silver et validate-gold (Great Expectations) sortent en erreur
uniquement sur une Expectation critical -- toutes celles de validate-gold
le sont (aucun invariant gold n'est un signal métier légitime, contrairement
à anomalie_distance en silver), voir great_expectations/validate_gold.py.

Planifié quotidiennement mais désactivé par défaut (AIRFLOW__CORE__DAGS_ARE_
PAUSED_AT_CREATION=true, réglé globalement dans docker-compose.yml) --
déclenchable manuellement à tout moment depuis l'interface, sans jamais
dépendre du planning automatique tant que le POC n'a pas besoin d'un vrai
rythme récurrent.

Toute tâche en échec déclenche une alerte Slack (voir slack_alerts.py) --
branché une seule fois via default_args, hérité par toutes les tâches.

Chaque tâche exécute "docker compose run --rm <service>" depuis le
conteneur scheduler lui-même (socket Docker monté, projet monté au même
chemin absolu dedans et dehors -- voir x-airflow-common dans
docker-compose.yml) -- pas de logique dupliquée entre Airflow et les
commandes qu'on lance déjà à la main.

spark-batch-silver tourne ici avec son comportement par défaut
(--source bronze, le flux continu) -- le rattrapage complet de l'historique
(--source postgres) reste une opération manuelle ponctuelle, hors DAG.
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from slack_alerts import notify_slack_failure

# -T : désactive l'allocation d'un pseudo-terminal -- nécessaire en
# exécution non-interactive (Airflow), sinon "docker compose run" peut
# échouer avec une erreur de type "the input device is not a TTY".
DOCKER_COMPOSE_RUN = "cd $PROJECT_ROOT && docker compose run --rm -T"

default_args = {
    "owner": "sportdata",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_slack_failure,
}

with DAG(
    dag_id="sportdata_pipeline",
    description="Pipeline récurrent : distances -> silver -> gold -> miroir PowerBI",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["sportdata"],
) as dag:

    compute_distances = BashOperator(
        task_id="compute_distances",
        bash_command=f"{DOCKER_COMPOSE_RUN} compute-distances",
    )

    spark_batch_silver = BashOperator(
        task_id="spark_batch_silver",
        bash_command=f"{DOCKER_COMPOSE_RUN} spark-batch-silver",
    )

    validate_silver = BashOperator(
        task_id="validate_silver",
        bash_command=f"{DOCKER_COMPOSE_RUN} validate-silver",
    )

    spark_batch_gold = BashOperator(
        task_id="spark_batch_gold",
        bash_command=f"{DOCKER_COMPOSE_RUN} spark-batch-gold",
    )

    validate_gold = BashOperator(
        task_id="validate_gold",
        bash_command=f"{DOCKER_COMPOSE_RUN} validate-gold",
    )

    sync_mirror = BashOperator(
        task_id="sync_mirror",
        bash_command=f"{DOCKER_COMPOSE_RUN} sync-mirror",
    )

    compute_distances >> spark_batch_silver >> validate_silver >> spark_batch_gold >> validate_gold >> sync_mirror
