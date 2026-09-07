"""
validate_source.py

Valide referentiel_rh, referentiel_sport et parametres_regles juste
après leur chargement (loader.py) -- rattaché à reset-complet.ps1, PAS
à sportdata_pipeline : les référentiels ne changent quasiment jamais
en dehors d'un chargement initial, donc pas de raison de revalider ça
à chaque exécution quotidienne du pipeline récurrent.

Moteur SQL direct (context.data_sources.add_postgres), pas pandas :
les deux Expectations sur parametres_regles utilisent du SQL
personnalisé (UnexpectedRowsExpectation), qui exige un moteur SQL --
pas disponible côté pandas. Cohérent d'utiliser le même moteur pour
les trois tables plutôt que d'en mélanger deux.

Trois Suites séparées, une par table -- une Suite est validée contre
un seul Batch, on ne peut pas mélanger des Expectations portant sur
des tables différentes dans une même Suite :

  - referentiel_rh :
      * effectif == 161 (warning, PAS critical) -- ce chiffre est une
        référence codée en dur, pas déduite dynamiquement (contrairement
        au comptage gold/référentiel de validate_gold.py). Un écart
        n'est jamais forcément un bug ici : la note de cadrage ne
        précise rien sur la gestion des embauches/départs, donc un
        écart peut être volontaire. Le rapport GX affiche observed_value
        à côté de la valeur attendue, ce qui suffit à lire le sens de
        l'écart (embauche si >, départ si <) sans logique dédiée. Si un
        écart s'avère volontaire, mettre à jour EFFECTIF_ATTENDU
        manuellement ci-dessous.
      * salaire_brut_annuel / nombre_jours_cp : plages de plausibilité
        (critical -- jamais un signal métier légitime, toujours un bug
        de saisie/import). Bornes élargies et arrondies autour de
        l'observé dans dataRH.xlsx (25 570-74 990 € / 25-29 jours),
        volontairement pas égales au min/max exact -- des bornes
        strictement égales à l'observé de CE chargement seraient
        tautologiques (toujours vraies sur ce jeu de données précis,
        aucune capacité de détection sur CE run).

  - referentiel_sport : taux de remplissage (warning, seuil bas) --
    remplissage normal déjà documenté ~59% (95/161), ce seuil ne sert
    qu'à détecter un chargement totalement raté, pas à juger le taux
    normal.

  - parametres_regles : exactement une ligne active, aucun chevauchement
    de périodes de validité SCD2 (critical, deux Expectations SQL
    personnalisées -- ni l'un ni l'autre n'est couvert par le schéma
    SQL actuel).

Suite construite entièrement en mémoire avant context.suites.add()
(même contournement que validate_silver.py/validate_gold.py pour le
bug de fraîcheur GX -- voir ces fichiers pour le détail).

Configuration via variables d'environnement :
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD (mêmes noms que les
    autres scripts -- défaut "localhost" pour une exécution hors Docker)
"""

import os
import sys
import tempfile

import great_expectations as gx
from great_expectations.expectations.metadata_types import FailureSeverity

PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "sportdata_source")
PGUSER = os.getenv("PGUSER", "sportdata")
PGPASSWORD = os.getenv("PGPASSWORD", "sportdata_pwd")

CONNECTION_STRING = f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"

# Codé en dur volontairement -- voir docstring. À mettre à jour à la
# main si un écart s'avère être une embauche/un départ confirmé.
EFFECTIF_ATTENDU = 161

SALAIRE_MIN = 15_000
SALAIRE_MAX = 100_000
CP_MIN = 20
CP_MAX = 35

SEUIL_REMPLISSAGE_SPORT = 0.3


def build_suite_referentiel_rh():
    suite = gx.core.expectation_suite.ExpectationSuite(name="source_referentiel_rh")

    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToEqual(value=EFFECTIF_ATTENDU, severity="warning")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="salaire_brut_annuel", min_value=SALAIRE_MIN, max_value=SALAIRE_MAX, severity="critical"
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="nombre_jours_cp", min_value=CP_MIN, max_value=CP_MAX, severity="critical"
        )
    )

    return suite


def build_suite_referentiel_sport():
    suite = gx.core.expectation_suite.ExpectationSuite(name="source_referentiel_sport")

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="sport_pratique", mostly=SEUIL_REMPLISSAGE_SPORT, severity="warning"
        )
    )

    return suite


def build_suite_parametres_regles():
    suite = gx.core.expectation_suite.ExpectationSuite(name="source_parametres_regles")

    suite.add_expectation(
        gx.expectations.UnexpectedRowsExpectation(
            unexpected_rows_query="""
                SELECT * FROM {batch}
                WHERE (SELECT COUNT(*) FROM {batch} WHERE date_fin_validite IS NULL) <> 1
            """,
            description="Il doit toujours exister exactement une ligne active (date_fin_validite IS NULL).",
            severity="critical",
        )
    )
    suite.add_expectation(
        gx.expectations.UnexpectedRowsExpectation(
            unexpected_rows_query="""
                SELECT a.* FROM {batch} a
                JOIN {batch} b ON a.parametre_id < b.parametre_id
                WHERE a.date_debut_validite < COALESCE(b.date_fin_validite, '9999-12-31')
                  AND b.date_debut_validite < COALESCE(a.date_fin_validite, '9999-12-31')
            """,
            description="Aucune période de validité ne doit chevaucher une autre (historique SCD2).",
            severity="critical",
        )
    )

    return suite


def run_validation(context, data_source, table_name, asset_name, suite):
    data_asset = data_source.add_table_asset(name=asset_name, table_name=table_name)
    batch_definition = data_asset.add_batch_definition_whole_table(f"batch_{asset_name}")

    suite = context.suites.add(suite)

    validation_definition = context.validation_definitions.add(
        gx.core.validation_definition.ValidationDefinition(
            name=f"validation_{asset_name}",
            data=batch_definition,
            suite=suite,
        )
    )

    return validation_definition.run()


def main():
    context = gx.get_context(mode="file", project_root_dir=tempfile.mkdtemp())
    data_source = context.data_sources.add_postgres("source", connection_string=CONNECTION_STRING)

    resultats = {
        "referentiel_rh": run_validation(
            context, data_source, "referentiel_rh", "referentiel_rh", build_suite_referentiel_rh()
        ),
        "referentiel_sport": run_validation(
            context, data_source, "referentiel_sport", "referentiel_sport", build_suite_referentiel_sport()
        ),
        "parametres_regles": run_validation(
            context, data_source, "parametres_regles", "parametres_regles", build_suite_parametres_regles()
        ),
    }

    for nom_table, resultat in resultats.items():
        print(f"--- {nom_table} ---")
        print(resultat.describe())

    severites = [r.get_max_severity_failure() for r in resultats.values()]
    severites = [s for s in severites if s is not None]

    if FailureSeverity.CRITICAL in severites:
        print("\nÉchec(s) critique(s) détecté(s) -- arrêt.")
        sys.exit(1)
    elif severites:
        print(f"\nÉchec(s) non critique(s) détecté(s) ({severites}) -- signalé, non bloquant.")
    else:
        print("\nValidation des référentiels source OK.")


if __name__ == "__main__":
    main()
