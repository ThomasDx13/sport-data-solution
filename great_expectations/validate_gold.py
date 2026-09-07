"""
validate_gold.py

Valide la qualité de indicateurs_eligibilite (couche gold) avec Great
Expectations, en gate avant sync-mirror -- si une Expectation critique
échoue, le script sort en erreur (code retour != 0) et la tâche Airflow
qui l'appelle doit interrompre le DAG avant sync-mirror. Toutes les
Expectations de cette Suite sont critical : contrairement à
anomalie_distance en silver, aucun des invariants vérifiés ici ne
correspond à un signal métier légitime -- ce sont uniquement des bugs
de calcul ou de jointure.

Gold est en mode append (contrairement à silver) : la table accumule
tous les runs historiques. On ne valide donc que le DERNIER run_date
(le lot qui vient d'être produit) -- pas tout l'historique cumulé à
chaque exécution.

Deux vérifications structurelles, dynamiques plutôt que codées en dur,
même principe que les bornes de dates en silver :
  - Le nombre de lignes du dernier run doit égaler le COUNT(*) réel de
    referentiel_rh (référentiel pilote du LEFT JOIN dans
    gold_eligibilite.py) -- un écart n'est jamais une question métier
    ici, toujours un bug de jointure.
  - taux_prime_applique / seuil_bien_etre_applique doivent égaler les
    valeurs actuellement actives dans parametres_regles.

Ne relit pas via Spark, même principe que validate_silver.py : lecture
directe via `deltalake` (delta-rs), pandas, pas de JVM.

Contexte GX en mode "file" sur un dossier temporaire recréé à chaque
exécution (pas "ephemeral") -- contourne un bug connu de GX 1.21
(ResourceFreshnessAggregateError après plusieurs add_expectation() en
contexte ephemeral, cf. GX-2891). Un dossier temporaire neuf à chaque
run garde le même effet pratique que l'ephemeral (rien à nettoyer),
même choix que validate_silver.py.

Configuration via variables d'environnement :
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD (source, mêmes noms
    que les autres scripts -- défaut "localhost" pour une exécution
    hors Docker, PGHOST=postgres-source à surcharger en conteneur)
    DELTA_STORAGE_PATH -- racine locale du bind mount delta-storage/
    (défaut : ../delta-storage, relatif à ce script)
"""

import os
import sys
import tempfile

import great_expectations as gx
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
GOLD_PATH = os.path.join(DELTA_STORAGE_PATH, "indicateurs_eligibilite")

# Doit rester synchronisé avec NB_JOURS_BIEN_ETRE dans gold_eligibilite.py
# -- pas de module partagé entre les deux scripts (conteneurs séparés),
# donc pas de moyen d'éviter cette duplication pour l'instant.
NB_JOURS_BIEN_ETRE = 5


def get_source_connection():
    return psycopg2.connect(
        host=PGHOST, port=PGPORT, dbname=PGDATABASE,
        user=PGUSER, password=PGPASSWORD,
    )


def fetch_referentiel_count(cur):
    cur.execute("SELECT COUNT(*) FROM referentiel_rh")
    (n,) = cur.fetchone()
    return n


def fetch_parametres_actifs(cur):
    """Mêmes valeurs actives que celles lues par gold_eligibilite.py
    (date_fin_validite IS NULL) -- doivent se retrouver telles quelles
    dans chaque ligne du dernier run gold."""
    cur.execute("""
        SELECT taux_prime, seuil_bien_etre
        FROM parametres_regles
        WHERE date_fin_validite IS NULL
    """)
    row = cur.fetchone()
    if row is None:
        sys.exit("Aucun paramètre actif dans parametres_regles (date_fin_validite IS NULL).")
    return row


def read_gold_dataframe():
    if not os.path.exists(GOLD_PATH):
        sys.exit(f"Table gold introuvable : {GOLD_PATH}")
    df = DeltaTable(GOLD_PATH).to_pandas()
    if df.empty:
        sys.exit("indicateurs_eligibilite est vide -- aucun run gold à valider.")

    dernier_run = df["run_date"].max()
    df = df[df["run_date"] == dernier_run].reset_index(drop=True)

    # taux_prime_applique (DECIMAL côté Delta) arrive probablement en
    # Decimal via la conversion pyarrow -> pandas, non comparable de façon
    # fiable à un float (Decimal('0.05') != 0.05 en Python) -- normalisé
    # ici plutôt que de deviner le type exact rencontré.
    df["taux_prime_applique"] = df["taux_prime_applique"].astype(float)

    return df


def build_expectation_suite(n_referentiel, taux_prime_actif, seuil_bien_etre_actif):
    # Suite construite entièrement en mémoire, PAS encore enregistrée dans
    # le context à ce stade -- si elle l'était, chaque add_expectation()
    # ci-dessous déclencherait une écriture immédiate et séparée dans le
    # store (comportement documenté de GX), ce qui semble être la vraie
    # cause du ResourceFreshnessAggregateError rencontré même après
    # suite.save() et en contexte "file". Un seul context.suites.add(),
    # une fois la Suite complète, dans main().
    suite = gx.core.expectation_suite.ExpectationSuite(name="gold_indicateurs_eligibilite")

    # Structurel : le référentiel RH pilote le LEFT JOIN dans
    # gold_eligibilite.py, donc un écart de comptage n'est jamais une
    # question métier -- toujours un bug de jointure.
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToEqual(value=n_referentiel, severity="critical")
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="employee_id", severity="critical")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="employee_id", severity="critical")
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="eligible_prime", severity="critical")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="eligible_bien_etre", severity="critical")
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="nb_jours_bien_etre_accordes", value_set=[0, NB_JOURS_BIEN_ETRE], severity="critical"
        )
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="cout_prime", min_value=0, severity="critical"
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="cout_jours_bien_etre", min_value=0, severity="critical"
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="nb_activites_12_mois", min_value=0, severity="critical"
        )
    )

    # Dynamique, comme le nombre de lignes -- doit égaler la config
    # actuellement active dans parametres_regles, pas une valeur figée.
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="taux_prime_applique", value_set=[taux_prime_actif], severity="critical"
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="seuil_bien_etre_applique", value_set=[seuil_bien_etre_actif], severity="critical"
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
            n_referentiel = fetch_referentiel_count(cur)
            taux_prime_actif, seuil_bien_etre_actif = fetch_parametres_actifs(cur)
            # float() explicite : psycopg2 renvoie un Decimal pour NUMERIC,
            # non comparable de façon fiable au float de la colonne pandas
            # (Decimal('0.05') != 0.05 en Python, imprécision binaire).
            taux_prime_actif = float(taux_prime_actif)
    finally:
        conn.close()

    df = read_gold_dataframe()
    print(f"{len(df)} ligne(s) lue(s) dans indicateurs_eligibilite (run_date : {df['run_date'].iloc[0]}).")

    context = gx.get_context(mode="file", project_root_dir=tempfile.mkdtemp())
    data_source = context.data_sources.add_pandas("gold")
    data_asset = data_source.add_dataframe_asset(name="indicateurs_eligibilite")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("batch")

    suite = build_expectation_suite(n_referentiel, taux_prime_actif, seuil_bien_etre_actif)
    suite = context.suites.add(suite)

    validation_definition = context.validation_definitions.add(
        gx.core.validation_definition.ValidationDefinition(
            name="validation_gold_indicateurs_eligibilite",
            data=batch_definition,
            suite=suite,
        )
    )

    result = validation_definition.run(batch_parameters={"dataframe": df})
    print(result.describe())

    max_severity = result.get_max_severity_failure()
    if max_severity == FailureSeverity.CRITICAL:
        print("\nÉchec(s) critique(s) détecté(s) -- arrêt du pipeline.")
        sys.exit(1)
    elif max_severity is not None:
        print(f"\nÉchec(s) non critique(s) détecté(s) ({max_severity}) -- signalé, pipeline non bloqué.")
    else:
        print("\nValidation gold OK.")


if __name__ == "__main__":
    main()
