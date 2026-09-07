"""
silver_enrich.py

Job Spark BATCH incrémental : enrichit les nouvelles activités (pas
encore présentes en silver) avec les référentiels RH/Sport et la
distance domicile-travail, et corrige les coquilles connues (ex.
"Runing" -> "Running").

Deux sources possibles pour les activités à traiter, mêmes jointures
et même logique d'enrichissement dans les deux cas :
  --source bronze   (défaut) -- lit activites_brutes (Delta), le flux
                     continu alimenté par Debezium/Redpanda/live.
  --source postgres -- lit activites_sportives directement via JDBC,
                     pour rattraper l'historique généré en masse par
                     generateur-historique. Ces activités ne passent
                     jamais par Redpanda (snapshot.mode=no_data côté
                     Debezium, décision volontaire), donc jamais par
                     le bronze non plus -- ce chemin de lecture directe
                     est le seul moyen de les enrichir.

Dans les deux cas, un anti-join contre activites_enrichies existant
évite tout doublon si jamais une même activité apparaissait des deux
côtés (activity_id vient de la même séquence Postgres quelle que soit
la voie empruntée).

Incrémental, pas un recalcul complet : à la différence du gold, qui
doit tout recalculer pour permettre le rejeu si un taux/seuil change,
rien ne justifie ici de refaire des jointures déjà faites.

Le bronze n'est jamais modifié -- il reste le reflet fidèle de ce qui
a été capturé. La correction orthographique se fait uniquement ici.

distance_domicile_travail_km et anomalie_distance viennent de la table
distances_domicile_travail (déjà calculée par compute_distances.py,
une fois par salarié) -- ce job ne fait aucun appel réseau lui-même,
juste une jointure.

Configuration DB via variables d'environnement (mêmes noms que le loader) :
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date

PGHOST = os.getenv("PGHOST", "postgres-source")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "sportdata_source")
PGUSER = os.getenv("PGUSER", "sportdata")
PGPASSWORD = os.getenv("PGPASSWORD", "sportdata_pwd")

JDBC_URL = f"jdbc:postgresql://{PGHOST}:{PGPORT}/{PGDATABASE}"
JDBC_PROPERTIES = {
    "user": PGUSER,
    "password": PGPASSWORD,
    "driver": "org.postgresql.Driver",
}

BRONZE_PATH = "/opt/delta/analytics/activites_brutes"
SILVER_PATH = "/opt/delta/analytics/activites_enrichies"

# Coquilles connues dans les données source -- corrigées ici, nulle part
# en amont (ni dans le générateur, ni en bronze). Extensible si d'autres
# coquilles apparaissent un jour dans les données réelles.
CORRECTIONS_ORTHOGRAPHE = {"Runing": "Running"}


def build_spark_session():
    return (
        SparkSession.builder
        .appName("sportdata-silver-enrich")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def ensure_silver_table_exists(spark):
    """Schéma de référence : sql/delta/01_schema_delta_lake.sql —
    cette création (idempotente, IF NOT EXISTS) s'y conforme."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta.`{SILVER_PATH}` (
            activity_id                     BIGINT,
            employee_id                     INT,
            date_debut_activite             TIMESTAMP,
            type_sport                      STRING,
            distance_m                      INT,
            date_fin_activite               TIMESTAMP,
            commentaire                     STRING,
            nom                              STRING,
            prenom                           STRING,
            salaire_brut_annuel              DECIMAL(10,2),
            mode_deplacement_declare         STRING,
            sport_pratique                   STRING,
            distance_domicile_travail_km     DECIMAL(6,2),
            anomalie_distance                 BOOLEAN,
            run_date                         DATE
        )
        USING DELTA
        PARTITIONED BY (run_date)
    """)


def read_postgres_table(spark, table_name):
    return (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_name)
        .options(**JDBC_PROPERTIES)
        .load()
    )


ACTIVITE_COLONNES_COMMUNES = [
    "activity_id", "employee_id", "date_debut_activite", "type_sport",
    "distance_m", "date_fin_activite", "commentaire",
]


def read_new_source_activities(spark, source):
    """Lit les activités depuis la source choisie, projetées sur les
    colonnes communes -- le reste du script ne sait pas d'où elles
    viennent, seulement qu'elles ont ces colonnes."""
    if source == "postgres":
        df = read_postgres_table(spark, "activites_sportives")
    else:
        df = spark.read.format("delta").load(BRONZE_PATH)
    return df.select(*ACTIVITE_COLONNES_COMMUNES)


def build_referentiels(spark):
    """Référentiels RH + Sport + distances, joints une seule fois,
    avec la coquille corrigée sur sport_pratique."""
    referentiel_rh = read_postgres_table(spark, "referentiel_rh")
    referentiel_sport = read_postgres_table(spark, "referentiel_sport")
    distances = read_postgres_table(spark, "distances_domicile_travail")

    return (
        referentiel_rh
        .join(referentiel_sport, on="employee_id", how="left")
        .join(distances, on="employee_id", how="left")
        .replace(CORRECTIONS_ORTHOGRAPHE, subset=["sport_pratique"])
        .select(
            "employee_id", "nom", "prenom", "salaire_brut_annuel",
            "mode_deplacement_declare", "sport_pratique",
            col("distance_domicile_travail_m").alias("_distance_domicile_travail_m"),
            "anomalie_distance",
        )
    )


def main():
    parser = argparse.ArgumentParser(description="Enrichissement silver — Sport Data Solution")
    parser.add_argument(
        "--source", choices=["bronze", "postgres"], default="bronze",
        help="bronze (flux continu, défaut) ou postgres (rattrapage direct de l'historique)",
    )
    args = parser.parse_args()

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    ensure_silver_table_exists(spark)

    source_activites = read_new_source_activities(spark, args.source)
    silver_existant = spark.read.format("delta").load(SILVER_PATH)

    nouvelles_activites = source_activites.join(
        silver_existant.select("activity_id"), on="activity_id", how="left_anti"
    )

    n_nouvelles = nouvelles_activites.count()
    if n_nouvelles == 0:
        print(f"Aucune nouvelle activité à traiter (source : {args.source}).")
        return

    print(f"{n_nouvelles} nouvelle(s) activité(s) à enrichir (source : {args.source}).")

    referentiels = build_referentiels(spark)

    enrichies = (
        nouvelles_activites
        .replace(CORRECTIONS_ORTHOGRAPHE, subset=["type_sport"])
        .join(referentiels, on="employee_id", how="left")
        .withColumn(
            "distance_domicile_travail_km",
            (col("_distance_domicile_travail_m") / 1000).cast("decimal(6,2)"),
        )
        .withColumn("run_date", current_date())
        .select(
            "activity_id", "employee_id", "date_debut_activite", "type_sport",
            "distance_m", "date_fin_activite", "commentaire",
            "nom", "prenom", "salaire_brut_annuel", "mode_deplacement_declare",
            "sport_pratique", "distance_domicile_travail_km", "anomalie_distance", "run_date",
        )
    )

    enrichies.write.format("delta").mode("append").partitionBy("run_date").save(SILVER_PATH)

    print(f"{n_nouvelles} activité(s) enrichie(s) et ajoutée(s) à activites_enrichies.")


if __name__ == "__main__":
    main()
