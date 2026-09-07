"""
load_referentiels.py

Charge (ou met à jour) les référentiels RH et Sport dans PostgreSQL
à partir des fichiers Excel sources (dataRH.xlsx, dataSport.xlsx).

Idempotent : peut être relancé autant de fois que nécessaire (upsert
via ON CONFLICT), sans jamais toucher à activites_sportives.

Configuration via variables d'environnement :
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
    RH_FILE, SPORT_FILE (chemins vers les .xlsx)
"""

import os
import sys

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "sportdata_source")
PGUSER = os.getenv("PGUSER", "sportdata")
PGPASSWORD = os.getenv("PGPASSWORD", "sportdata_pwd")

RH_FILE = os.getenv("RH_FILE", "/data/dataRH.xlsx")
SPORT_FILE = os.getenv("SPORT_FILE", "/data/dataSport.xlsx")


def load_excel_files():
    if not os.path.exists(RH_FILE):
        sys.exit(f"Fichier introuvable : {RH_FILE}")
    if not os.path.exists(SPORT_FILE):
        sys.exit(f"Fichier introuvable : {SPORT_FILE}")

    rh = pd.read_excel(RH_FILE)
    sport = pd.read_excel(SPORT_FILE)
    return rh, sport


def upsert_referentiel_rh(cur, rh):
    rows = [
        (
            int(r["ID salarié"]),
            r["Nom"],
            r["Prénom"],
            r["Date de naissance"].date(),
            r["BU"],
            r["Date d'embauche"].date(),
            int(r["Salaire brut"]),
            r["Type de contrat"],
            int(r["Nombre de jours de CP"]),
            r["Adresse du domicile"],
            r["Moyen de déplacement"],
        )
        for _, r in rh.iterrows()
    ]

    query = """
        INSERT INTO referentiel_rh
            (employee_id, nom, prenom, date_naissance, bu, date_embauche,
             salaire_brut_annuel, type_contrat, nombre_jours_cp, adresse,
             mode_deplacement_declare)
        VALUES %s
        ON CONFLICT (employee_id) DO UPDATE SET
            nom = EXCLUDED.nom,
            prenom = EXCLUDED.prenom,
            date_naissance = EXCLUDED.date_naissance,
            bu = EXCLUDED.bu,
            date_embauche = EXCLUDED.date_embauche,
            salaire_brut_annuel = EXCLUDED.salaire_brut_annuel,
            type_contrat = EXCLUDED.type_contrat,
            nombre_jours_cp = EXCLUDED.nombre_jours_cp,
            adresse = EXCLUDED.adresse,
            mode_deplacement_declare = EXCLUDED.mode_deplacement_declare,
            date_maj = now();
    """
    execute_values(cur, query, rows)
    return len(rows)


def upsert_referentiel_sport(cur, sport):
    rows = [
        (
            int(r["ID salarié"]),
            None if pd.isnull(r["Pratique d'un sport"]) else r["Pratique d'un sport"],
        )
        for _, r in sport.iterrows()
    ]

    query = """
        INSERT INTO referentiel_sport (employee_id, sport_pratique)
        VALUES %s
        ON CONFLICT (employee_id) DO UPDATE SET
            sport_pratique = EXCLUDED.sport_pratique,
            date_maj = now();
    """
    execute_values(cur, query, rows)
    return len(rows)


def main():
    rh, sport = load_excel_files()

    conn = psycopg2.connect(
        host=PGHOST, port=PGPORT, dbname=PGDATABASE,
        user=PGUSER, password=PGPASSWORD,
    )
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            n_rh = upsert_referentiel_rh(cur, rh)
            n_sport = upsert_referentiel_sport(cur, sport)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"referentiel_rh    : {n_rh} lignes chargées/mises à jour")
    print(f"referentiel_sport : {n_sport} lignes chargées/mises à jour")


if __name__ == "__main__":
    main()
