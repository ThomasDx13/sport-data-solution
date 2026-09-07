"""
gold_eligibilite.py

Job Spark BATCH : calcule les indicateurs d'éligibilité (prime,
jours bien-être) et l'impact financier pour tous les salariés, à
chaque exécution -- recalcul COMPLET (pas incrémental comme le
silver), pour permettre le rejeu si un taux/seuil change.

Sources :
  - referentiel_rh (JDBC) : les 161 salariés, salaire, mode déclaré --
    table pivot, garantit qu'aucun salarié n'est exclu même sans
    activité (contrairement au silver, qui n'a de ligne que pour les
    salariés ayant au moins une activité).
  - activites_enrichies (Delta, silver) : nombre d'activités sur les
    12 derniers mois, agrégé par salarié.
  - distances_domicile_travail (JDBC) : anomalie_distance, déjà
    calculée une fois par compute_distances.py -- pas recalculée ici.
  - parametres_regles (JDBC) : taux/seuil actuellement actifs
    (date_fin_validite IS NULL) -- source de vérité, figée dans
    chaque ligne produite pour garantir la rejouabilité.

Les deux avantages sont cumulables (confirmé).
Coût d'un jour de congé bien-être : salaire_brut_annuel / 218 jours
travaillés/an -- hypothèse documentée, rien de précisé à ce sujet
dans la note de cadrage.

Configuration DB via variables d'environnement (mêmes noms que le loader) :
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, count, current_date, current_timestamp, date_sub, lit, when

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

SILVER_PATH = "/opt/delta/analytics/activites_enrichies"
GOLD_PATH = "/opt/delta/analytics/indicateurs_eligibilite"

MODES_VERTS = ["Marche/running", "Vélo/Trottinette/Autres"]

JOURS_TRAVAILLES_PAR_AN = 218   # hypothèse documentée, non précisée dans la note de cadrage
NB_JOURS_BIEN_ETRE = 5
FENETRE_JOURS = 365


def build_spark_session():
    return (
        SparkSession.builder
        .appName("sportdata-gold-eligibilite")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def ensure_gold_table_exists(spark):
    """Schéma de référence : sql/delta/01_schema_delta_lake.sql."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta.`{GOLD_PATH}` (
            run_date                       TIMESTAMP,
            employee_id                    INT,
            nb_activites_12_mois           INT,
            eligible_prime                 BOOLEAN,
            eligible_bien_etre             BOOLEAN,
            nb_jours_bien_etre_accordes    INT,
            taux_prime_applique            DECIMAL(5,4),
            seuil_bien_etre_applique       INT,
            cout_prime                     DECIMAL(10,2),
            cout_jours_bien_etre           DECIMAL(10,2)
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


def read_parametres_actifs(spark):
    """La ligne actuellement active (date_fin_validite IS NULL). Erreur
    explicite si aucune ou plusieurs lignes actives -- une config
    ambiguë ne doit jamais passer silencieusement."""
    params = read_postgres_table(spark, "parametres_regles").filter(col("date_fin_validite").isNull())
    rows = params.collect()
    if len(rows) == 0:
        raise RuntimeError("Aucun paramètre actif dans parametres_regles (date_fin_validite IS NULL).")
    if len(rows) > 1:
        raise RuntimeError(f"{len(rows)} paramètres actifs simultanément dans parametres_regles -- config ambiguë.")
    return rows[0]["taux_prime"], rows[0]["seuil_bien_etre"]


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    ensure_gold_table_exists(spark)

    taux_prime, seuil_bien_etre = read_parametres_actifs(spark)
    print(f"Paramètres actifs : taux_prime={taux_prime}, seuil_bien_etre={seuil_bien_etre}")

    referentiel_rh = read_postgres_table(spark, "referentiel_rh").select(
        "employee_id", "salaire_brut_annuel", "mode_deplacement_declare"
    )
    distances = read_postgres_table(spark, "distances_domicile_travail").select(
        "employee_id", "anomalie_distance"
    )

    fenetre_debut = date_sub(current_date(), FENETRE_JOURS)
    silver = spark.read.format("delta").load(SILVER_PATH)
    activites_recentes = (
        silver
        .filter(col("date_debut_activite") >= fenetre_debut)
        .groupBy("employee_id")
        .agg(count("*").alias("nb_activites_12_mois"))
    )

    resultat = (
        referentiel_rh
        .join(distances, on="employee_id", how="left")
        .join(activites_recentes, on="employee_id", how="left")
        # coalesce systématique après les LEFT JOIN : un salarié sans activité
        # récente ou sans distance calculée (mode non vert) ne doit jamais
        # produire de NULL qui se propagerait dans les AND/comparaisons
        # suivants -- 0 activité et "pas d'anomalie" sont les valeurs neutres
        # correctes dans ces deux cas, pas une absence de donnée à traiter.
        .withColumn("nb_activites_12_mois", coalesce(col("nb_activites_12_mois"), lit(0)).cast("int"))
        .withColumn("anomalie_distance", coalesce(col("anomalie_distance"), lit(False)))
        .withColumn(
            "eligible_prime",
            col("mode_deplacement_declare").isin(MODES_VERTS) & (~col("anomalie_distance"))
        )
        .withColumn("eligible_bien_etre", col("nb_activites_12_mois") >= lit(seuil_bien_etre))
        .withColumn(
            "nb_jours_bien_etre_accordes",
            when(col("eligible_bien_etre"), lit(NB_JOURS_BIEN_ETRE)).otherwise(lit(0)).cast("int")
        )
        .withColumn("taux_prime_applique", lit(taux_prime).cast("decimal(5,4)"))
        .withColumn("seuil_bien_etre_applique", lit(seuil_bien_etre).cast("int"))
        .withColumn(
            "cout_prime",
            when(col("eligible_prime"), col("salaire_brut_annuel") * lit(taux_prime))
            .otherwise(lit(0)).cast("decimal(10,2)")
        )
        .withColumn(
            "cout_jours_bien_etre",
            (col("salaire_brut_annuel") / lit(JOURS_TRAVAILLES_PAR_AN) * col("nb_jours_bien_etre_accordes"))
            .cast("decimal(10,2)")
        )
        .withColumn("run_date", current_timestamp())
        .select(
            "run_date", "employee_id", "nb_activites_12_mois",
            "eligible_prime", "eligible_bien_etre", "nb_jours_bien_etre_accordes",
            "taux_prime_applique", "seuil_bien_etre_applique",
            "cout_prime", "cout_jours_bien_etre",
        )
    )

    resultat.write.format("delta").mode("append").partitionBy("run_date").save(GOLD_PATH)

    n_total = resultat.count()
    n_eligible_prime = resultat.filter(col("eligible_prime")).count()
    n_eligible_bien_etre = resultat.filter(col("eligible_bien_etre")).count()
    cout_total = resultat.agg({"cout_prime": "sum", "cout_jours_bien_etre": "sum"}).collect()[0]

    print(f"{n_total} salarié(s) traité(s)")
    print(f"  éligibles prime : {n_eligible_prime}")
    print(f"  éligibles bien-être : {n_eligible_bien_etre}")
    print(f"  coût total prime : {cout_total['sum(cout_prime)']:.2f} €")
    print(f"  coût total jours bien-être : {cout_total['sum(cout_jours_bien_etre)']:.2f} €")


if __name__ == "__main__":
    main()
