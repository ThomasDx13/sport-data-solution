"""
streaming_bronze.py

Job Spark Structured Streaming : consomme le topic Redpanda
sportdata.public.activites_sportives (alimenté par Debezium) et
écrit en continu dans la table Delta BRONZE activites_brutes.

Aucune transformation métier ici (pas de jointure, pas de calcul,
pas de correction de coquille) — seulement le typage des champs et
les métadonnées techniques nécessaires à la traçabilité et au rejeu
(partition/offset Kafka, date d'ingestion). L'enrichissement et le
nettoyage (SILVER) sont un job séparé.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_date
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, StringType,
)

KAFKA_BOOTSTRAP_SERVERS = "redpanda:9092"
KAFKA_TOPIC = "sportdata.public.activites_sportives"
DELTA_TABLE_PATH = "/opt/delta/analytics/activites_brutes"
CHECKPOINT_PATH = "/opt/spark/checkpoints/activites_brutes"

# Format fixé côté connecteur Debezium (transforms.convertXxx.format) —
# les deux doivent rester synchronisés si l'un des deux change un jour.
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"

# Schéma du contenu utile, une fois descendu dans "payload" (voir plus bas).
# Champs conformes à la note de cadrage : distance en mètres, date de fin
# (pas une durée), commentaire. distance_m est un entier simple -- plus
# besoin de gérer un décimal Debezium (Base64) comme avec l'ancien
# distance_km, la colonne Postgres est un INTEGER, pas un NUMERIC.
MESSAGE_SCHEMA = StructType([
    StructField("activity_id", LongType()),
    StructField("employee_id", IntegerType()),
    StructField("date_debut_activite", StringType()),
    StructField("type_sport", StringType()),
    StructField("distance_m", IntegerType()),
    StructField("date_fin_activite", StringType()),
    StructField("commentaire", StringType()),
    StructField("created_at", StringType()),
    StructField("__op", StringType()),
])

# Le message Kafka réel est enveloppé sous la forme {"schema": {...}, "payload": {...}} —
# malgré VALUE_CONVERTER_SCHEMAS_ENABLE=false côté Debezium, qui était censé produire du
# JSON plat sans cette enveloppe. Vérifié sur un message brut réel (Redpanda Console) :
# l'effet attendu ne se produit pas, cause non élucidée pour l'instant. On s'adapte à la
# forme réelle plutôt que de continuer à chercher pourquoi le réglage semble ignoré —
# le "schema" n'est pas modélisé ici, from_json en mode permissif l'ignore simplement
# puisqu'il n'apparaît pas dans KAFKA_MESSAGE_SCHEMA.
KAFKA_MESSAGE_SCHEMA = StructType([
    StructField("payload", MESSAGE_SCHEMA),
])


def build_spark_session():
    return (
        SparkSession.builder
        .appName("sportdata-streaming-bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def ensure_bronze_table_exists(spark):
    """Le schéma SQL de référence est sql/delta/01_schema_delta_lake.sql —
    cette création (idempotente, IF NOT EXISTS) s'y conforme plutôt que
    de laisser Delta déduire un schéma depuis le DataFrame en écriture."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta.`{DELTA_TABLE_PATH}` (
            activity_id            BIGINT,
            employee_id            INT,
            date_debut_activite    TIMESTAMP,
            type_sport             STRING,
            distance_m             INT,
            date_fin_activite      TIMESTAMP,
            commentaire            STRING,
            created_at             TIMESTAMP,
            op                     STRING,
            kafka_partition        INT,
            kafka_offset           BIGINT,
            ingestion_date         DATE
        )
        USING DELTA
        PARTITIONED BY (ingestion_date)
    """)


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    ensure_bronze_table_exists(spark)

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw
        .select(
            from_json(col("value").cast("string"), KAFKA_MESSAGE_SCHEMA).alias("envelope"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
        )
        .select("envelope.payload.*", "kafka_partition", "kafka_offset")
        .withColumn("date_debut_activite", to_timestamp(col("date_debut_activite"), TIMESTAMP_FORMAT))
        .withColumn("date_fin_activite", to_timestamp(col("date_fin_activite"), TIMESTAMP_FORMAT))
        .withColumn("created_at", to_timestamp(col("created_at"), TIMESTAMP_FORMAT))
        .withColumnRenamed("__op", "op")
        .withColumn("ingestion_date", current_date())
    )

    query = (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .partitionBy("ingestion_date")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start(DELTA_TABLE_PATH)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
