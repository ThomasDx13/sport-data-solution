"""
sync_mirror.py

Synchronise le miroir PostgreSQL (postgres-mirror, consommé par PowerBI)
depuis les sources de vérité : référentiels + paramètres (postgres-source,
JDBC) et indicateurs d'éligibilité (Delta, gold).

Trois tables, trois logiques de synchronisation différentes :
  - dim_salarie           : écrasement complet à chaque run (TRUNCATE +
                             réécriture). Coquilles corrigées sur
                             sport_pratique -- le silver ne peut pas
                             servir de source ici, il n'a de ligne que
                             pour les salariés ayant au moins une
                             activité, alors que dim_salarie doit
                             couvrir les 161.
  - dim_parametres_regles : écrasement complet -- table petite, tout
                             l'historique SCD2 copié tel quel.
  - fait_eligibilite      : INCRÉMENTAL par run_date -- chaque run du
                             gold produit un snapshot déjà figé ; on
                             ajoute les run_date pas encore
                             synchronisés, jamais on ne retouche les
                             anciens.

Le mode "overwrite" par défaut de Spark en JDBC fait un DROP TABLE +
recréation avec un schéma déduit du DataFrame -- ça écraserait
silencieusement les contraintes écrites à la main dans
sql/mirror/01_schema_mirror_postgresql.sql. truncate=true est donc
explicite pour dim_salarie et dim_parametres_regles.

Configuration via variables d'environnement :
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD (source, mêmes noms que le loader)
    MIRROR_HOST, MIRROR_PORT, MIRROR_DATABASE, MIRROR_USER, MIRROR_PASSWORD (miroir)
"""

import os

from pyspark.sql import SparkSession

from corrections import CORRECTIONS_ORTHOGRAPHE

PGHOST = os.getenv("PGHOST", "postgres-source")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "sportdata_source")
PGUSER = os.getenv("PGUSER", "sportdata")
PGPASSWORD = os.getenv("PGPASSWORD", "sportdata_pwd")

MIRROR_HOST = os.getenv("MIRROR_HOST", "postgres-mirror")
MIRROR_PORT = os.getenv("MIRROR_PORT", "5432")   # port INTERNE au réseau Docker, pas le 5433 exposé côté hôte
MIRROR_DATABASE = os.getenv("MIRROR_DATABASE", "sportdata_mirror")
MIRROR_USER = os.getenv("MIRROR_USER", "sportdata")
MIRROR_PASSWORD = os.getenv("MIRROR_PASSWORD", "sportdata_pwd")

SOURCE_JDBC_URL = f"jdbc:postgresql://{PGHOST}:{PGPORT}/{PGDATABASE}"
SOURCE_JDBC_PROPERTIES = {"user": PGUSER, "password": PGPASSWORD, "driver": "org.postgresql.Driver"}

MIRROR_JDBC_URL = f"jdbc:postgresql://{MIRROR_HOST}:{MIRROR_PORT}/{MIRROR_DATABASE}"
MIRROR_JDBC_PROPERTIES = {"user": MIRROR_USER, "password": MIRROR_PASSWORD, "driver": "org.postgresql.Driver"}

GOLD_PATH = "/opt/delta/analytics/indicateurs_eligibilite"


def build_spark_session():
    return (
        SparkSession.builder
        .appName("sportdata-sync-mirror")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def read_source_table(spark, table_name):
    return (
        spark.read.format("jdbc")
        .option("url", SOURCE_JDBC_URL)
        .option("dbtable", table_name)
        .options(**SOURCE_JDBC_PROPERTIES)
        .load()
    )


def read_mirror_table(spark, table_name):
    return (
        spark.read.format("jdbc")
        .option("url", MIRROR_JDBC_URL)
        .option("dbtable", table_name)
        .options(**MIRROR_JDBC_PROPERTIES)
        .load()
    )


def overwrite_mirror_table(df, table_name):
    """TRUNCATE explicite -- pas le comportement 'overwrite' par défaut
    de Spark (DROP + recréation avec un schéma déduit du DataFrame),
    qui écraserait les contraintes définies à la main sur cette table."""
    (
        df.write.format("jdbc")
        .option("url", MIRROR_JDBC_URL)
        .option("dbtable", table_name)
        .option("truncate", "true")
        .options(**MIRROR_JDBC_PROPERTIES)
        .mode("overwrite")
        .save()
    )


def sync_dim_salarie(spark):
    referentiel_rh = read_source_table(spark, "referentiel_rh").select(
        "employee_id", "nom", "prenom", "bu", "type_contrat",
        "salaire_brut_annuel", "mode_deplacement_declare",
    )
    referentiel_sport = (
        read_source_table(spark, "referentiel_sport")
        .select("employee_id", "sport_pratique")
        .replace(CORRECTIONS_ORTHOGRAPHE, subset=["sport_pratique"])
    )

    dim_salarie = referentiel_rh.join(referentiel_sport, on="employee_id", how="left")
    overwrite_mirror_table(dim_salarie, "dim_salarie")
    print(f"dim_salarie : {dim_salarie.count()} ligne(s) synchronisée(s) (écrasement complet).")


def sync_dim_parametres_regles(spark):
    parametres = read_source_table(spark, "parametres_regles")
    overwrite_mirror_table(parametres, "dim_parametres_regles")
    print(f"dim_parametres_regles : {parametres.count()} ligne(s) synchronisée(s) (écrasement complet).")


def sync_fait_eligibilite(spark):
    gold = spark.read.format("delta").load(GOLD_PATH)

    run_dates_existants = [
        row["run_date"]
        for row in read_mirror_table(spark, "fait_eligibilite").select("run_date").distinct().collect()
    ]

    if run_dates_existants:
        nouvelles_lignes = gold.filter(~gold["run_date"].isin(run_dates_existants))
    else:
        nouvelles_lignes = gold

    n_nouvelles = nouvelles_lignes.count()
    if n_nouvelles == 0:
        print("fait_eligibilite : aucun nouveau run_date à synchroniser.")
        return

    (
        nouvelles_lignes.write.format("jdbc")
        .option("url", MIRROR_JDBC_URL)
        .option("dbtable", "fait_eligibilite")
        .options(**MIRROR_JDBC_PROPERTIES)
        .mode("append")
        .save()
    )
    print(f"fait_eligibilite : {n_nouvelles} nouvelle(s) ligne(s) synchronisée(s) (incrémental par run_date).")


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    sync_dim_salarie(spark)          # avant fait_eligibilite : PowerBI a besoin des salariés déjà présents
    sync_dim_parametres_regles(spark)
    sync_fait_eligibilite(spark)


if __name__ == "__main__":
    main()
