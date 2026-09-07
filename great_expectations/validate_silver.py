"""
validate_silver.py

Valide la qualité de activites_enrichies (couche silver) avec Great
Expectations, en gate entre silver et gold : si une Expectation
critique échoue, le script sort en erreur (code retour != 0) et la
tâche Airflow qui l'appelle doit interrompre le DAG avant gold. Les
échecs "warning" (ex. anomalie_distance) sont journalisés mais ne
bloquent pas -- cohérent avec compute_distances.py, qui insère déjà
volontairement les anomalies plutôt que d'échouer.

NB (voir échange avec Thomas) : la sévérité ("critical" vs "warning")
n'a pas de confirmation documentaire quant à son effet réel sur
result.success -- ce script sort en erreur dès que result.success est
False, quelle que soit la sévérité, en attendant un test réel pour
confirmer ou ajuster ce comportement.

Ne relit pas via Spark : delta-storage/ est un bind mount local (déjà
utilisé par DuckDB pour l'inspection), donc lu ici directement via le
package `deltalake` (delta-rs), sans JVM ni session Spark -- le volume
de données (~18 500 lignes) tient largement en mémoire.

Contexte GX en mode "file" sur un dossier temporaire recréé à chaque
exécution (pas "ephemeral") -- contourne un bug connu de GX 1.21
(ResourceFreshnessAggregateError après plusieurs add_expectation() en
contexte ephemeral, cf. GX-2891). Un dossier temporaire neuf à chaque
run garde le même effet pratique que l'ephemeral (rien à nettoyer, la
Suite est reconstruite intégralement à chaque fois, cohérent avec le
recalcul complet déjà appliqué au gold).

Les bornes de dates utilisées dans les Expectations sont calculées
dynamiquement (plage de dates plausible = min historique - marge, à
now + tolérance) -- jamais codées en dur.

Configuration via variables d'environnement :
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD (source, mêmes noms
    que les autres scripts -- défaut "localhost" : ce script est fait
    pour tourner hors du réseau Docker, comme compute_distances.py)
    DELTA_STORAGE_PATH -- racine locale du bind mount delta-storage/
    (défaut : ../delta-storage, relatif à ce script)
"""

import os
import sys
import tempfile
from datetime import datetime, timedelta

import great_expectations as gx
import pandas as pd
import psycopg2
from deltalake import DeltaTable
from great_expectations.expectations.metadata_types import FailureSeverity

PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "sportdata_source")
PGUSER = os.getenv("PGUSER", "sportdata")
PGPASSWORD = os.getenv("PGPASSWORD", "sportdata_pwd")

DELTA_STORAGE_PATH = os.getenv(
    "DELTA_STORAGE_PATH",
    os.path.join(os.path.dirname(__file__), "..", "delta-storage"),
)
SILVER_PATH = os.path.join(DELTA_STORAGE_PATH, "activites_enrichies")

# Marge sous le minimum historique réel, tolérance au-dessus de "maintenant"
# -- absorbe un décalage d'horloge éventuel, pas une vraie fenêtre métier.
MARGE_BORNE_BASSE = timedelta(days=1)
TOLERANCE_BORNE_HAUTE = timedelta(hours=6)


def get_source_connection():
    return psycopg2.connect(
        host=PGHOST, port=PGPORT, dbname=PGDATABASE,
        user=PGUSER, password=PGPASSWORD,
    )


def fetch_bornes_dates_dynamiques(cur):
    """Bornes de plausibilité pour date_debut_activite / date_fin_activite,
    calculées depuis l'historique réel en base plutôt que codées en dur --
    reste valable même si la fenêtre de génération change."""
    cur.execute("SELECT MIN(date_debut_activite) FROM activites_sportives")
    (min_historique,) = cur.fetchone()
    if min_historique is None:
        sys.exit("activites_sportives est vide -- impossible de calculer une borne basse.")

    borne_basse = min_historique - MARGE_BORNE_BASSE
    borne_haute = datetime.now() + TOLERANCE_BORNE_HAUTE
    return borne_basse, borne_haute


def read_silver_dataframe():
    if not os.path.exists(SILVER_PATH):
        sys.exit(f"Table silver introuvable : {SILVER_PATH}")
    df = DeltaTable(SILVER_PATH).to_pandas()

    # deltalake (delta-rs) restitue les colonnes timestamp en tz-aware UTC
    # (convention Parquet/Delta), alors que nos bornes de comparaison
    # (psycopg2, datetime.now()) sont naïves -- sans cette normalisation,
    # ExpectColumnValuesToBeBetween échoue silencieusement (résultat vide)
    # au lieu de comparer réellement les valeurs.
    for colonne in ("date_debut_activite", "date_fin_activite"):
        if pd.api.types.is_datetime64tz_dtype(df[colonne]):
            df[colonne] = df[colonne].dt.tz_localize(None)

    return df


def build_expectation_suite(borne_basse, borne_haute):
    # Suite construite entièrement en mémoire, PAS encore enregistrée dans
    # le context à ce stade -- voir le commentaire équivalent dans
    # validate_gold.py pour le détail du bug contourné ici.
    suite = gx.core.expectation_suite.ExpectationSuite(name="silver_activites_enrichies")

    # Identifiants
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="activity_id", severity="critical")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="activity_id", severity="critical")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="employee_id", severity="critical")
    )

    # Cohérence de la jointure référentiel -- un employee_id orphelin
    # laisserait nom/prenom à NULL après le left join
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="nom", severity="critical")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="prenom", severity="critical")
    )

    # Timestamps
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="date_debut_activite", severity="critical")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="date_fin_activite", severity="critical")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="date_fin_activite", column_B="date_debut_activite",
            or_equal=True, severity="critical",
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="date_debut_activite",
            min_value=borne_basse, max_value=borne_haute,
            severity="critical",
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="date_fin_activite",
            min_value=borne_basse, max_value=borne_haute,
            severity="critical",
        )
    )

    # Distance de l'activité elle-même -- NULL attendu pour certains
    # sports (ex. escalade), ignoré par l'Expectation par défaut
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="distance_m", min_value=0, strict_min=True, severity="critical"
        )
    )

    # Salaire
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="salaire_brut_annuel", severity="critical")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="salaire_brut_annuel", min_value=0, strict_min=True, severity="critical"
        )
    )

    # Coquilles -- pas de liste exhaustive des valeurs valides (inconnue
    # à l'avance), seulement la certitude que la coquille corrigée ne
    # doit plus jamais réapparaître après le passage en silver
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeInSet(
            column="type_sport", value_set=["Runing"], severity="critical"
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeInSet(
            column="sport_pratique", value_set=["Runing"], severity="critical"
        )
    )

    # Distance domicile-travail -- NULL attendu pour les modes non-verts,
    # ignoré par l'Expectation comme distance_m plus haut
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="distance_domicile_travail_km", min_value=0, strict_min=True, severity="critical"
        )
    )

    # Anomalie de distance -- warning, pas critical : compute_distances.py
    # insère volontairement les anomalies plutôt que d'échouer, la Suite
    # doit signaler sans bloquer, pour rester cohérente avec ce choix
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="anomalie_distance", value_set=[False], severity="warning"
        )
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="run_date", severity="critical")
    )

    return suite


def main():
    conn = get_source_connection()
    try:
        with conn.cursor() as cur:
            borne_basse, borne_haute = fetch_bornes_dates_dynamiques(cur)
    finally:
        conn.close()

    df = read_silver_dataframe()
    print(f"{len(df)} ligne(s) lue(s) dans activites_enrichies.")

    context = gx.get_context(mode="file", project_root_dir=tempfile.mkdtemp())
    data_source = context.data_sources.add_pandas("silver")
    data_asset = data_source.add_dataframe_asset(name="activites_enrichies")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("batch")

    suite = build_expectation_suite(borne_basse, borne_haute)
    suite = context.suites.add(suite)

    validation_definition = context.validation_definitions.add(
        gx.core.validation_definition.ValidationDefinition(
            name="validation_silver_activites_enrichies",
            data=batch_definition,
            suite=suite,
        )
    )

    # Appel direct de la Validation Definition plutôt que de passer par un
    # Checkpoint : on n'utilise aucune fonctionnalité propre au Checkpoint
    # (action_list, Data Docs) pour l'instant, et ça renvoie directement un
    # ExpectationSuiteValidationResult -- l'objet dont get_max_severity_failure()
    # est confirmé dans la doc courante (1.21.0), contrairement à la structure
    # exacte de CheckpointResult que je n'ai pas pu vérifier avec la même
    # certitude pour cette version.
    result = validation_definition.run(batch_parameters={"dataframe": df})
    print(result.describe())

    # Distinction réelle critical/warning, confirmée par test (UPDATE manuel
    # de anomalie_distance) : un échec warning seul NE DOIT PAS arrêter le
    # pipeline, cohérent avec compute_distances.py qui ne fait jamais
    # échouer son propre job sur une anomalie de distance.
    max_severity = result.get_max_severity_failure()
    if max_severity == FailureSeverity.CRITICAL:
        print("\nÉchec(s) critique(s) détecté(s) -- arrêt du pipeline.")
        sys.exit(1)
    elif max_severity is not None:
        print(f"\nÉchec(s) non critique(s) détecté(s) ({max_severity}) -- signalé, pipeline non bloqué.")
    else:
        print("\nValidation silver OK.")


if __name__ == "__main__":
    main()
