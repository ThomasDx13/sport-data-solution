"""
generate_activites.py

Génère l'historique de 12 mois d'activités sportives pour les 161 salariés,
à partir des référentiels déjà chargés dans PostgreSQL (pas des .xlsx —
la base est la source de vérité une fois le loader passé).

Champs produits conformes à la note de cadrage : ID, ID salarié, date de
début, type, distance (en mètres, NULL si non pertinent), date de fin,
commentaire. La durée n'est qu'un paramètre interne de génération -- elle
n'est jamais stockée telle quelle, seule (date_fin - date_debut) l'exprime.

Deux familles d'activités générées indépendamment l'une de l'autre :
  - Trajet domicile-travail : uniquement si mode_deplacement_declare est
    un mode "vert" (Marche/running, Vélo/Trottinette/Autres).
  - Loisir sportif : TOUS les salariés en génèrent, déclarants ou non.
    - Sport déclaré (95/161)   -> ce sport, engagement 0.2 à 1.0
    - Sport non déclaré (66/161) -> un sport tiré au hasard, pondéré par
      la fréquence réelle des déclarations observées dans le référentiel
      (Runing, Randonnée, Tennis... les plus déclarés ont plus de chances
      d'être tirés), engagement 0.0 à 0.3 (pratique occasionnelle : un
      padel avec des collègues, un foot de temps en temps...).
    Le sport tiré pour un non-déclarant n'est jamais écrit dans
    referentiel_sport -- ce n'est pas une déclaration, juste un artefact
    de génération pour produire un historique plausible.

Modes :
  --mode historique  : génère 12 mois d'historique pour tous les salariés.
                        Idempotent : si la table contient déjà des lignes,
                        ne fait rien (skip informatif, sortie 0) sauf
                        --force explicite, qui vide et régénère. Ce choix
                        (skip plutôt qu'échec dur) permet à ce script
                        d'être rejoué sans risque dans un script
                        d'orchestration (bootstrap.ps1) -- pour forcer
                        une vraie régénération volontaire, --force reste
                        le seul moyen, jamais le comportement par défaut.
  --mode live         : insère une activité "maintenant", pour un salarié
                        donné ou tiré au hasard

Configuration DB via variables d'environnement (mêmes noms que le loader) :
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
"""

import argparse
import os
import random
import sys
from collections import Counter
from datetime import date, datetime, timedelta

import psycopg2
from psycopg2.extras import execute_values

PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "sportdata_source")
PGUSER = os.getenv("PGUSER", "sportdata")
PGPASSWORD = os.getenv("PGPASSWORD", "sportdata_pwd")

# ------------------------------------------------------------------
# Paramètres de génération — hypothèses documentées, ajustables ici
# sans toucher à la logique du script. Distances en MÈTRES (conforme
# à la note de cadrage), durées en minutes (paramètre interne, non
# stocké -- sert uniquement à calculer date_fin_activite).
# ------------------------------------------------------------------

COMMUTE_PARAMS = {
    "Marche/running":          {"distance": (1000, 8000),  "duree": (10, 60), "prob_jour_ouvre": 0.7},
    "Vélo/Trottinette/Autres": {"distance": (3000, 20000), "duree": (10, 45), "prob_jour_ouvre": 0.7},
}

LEISURE_PARAMS = {
    "Runing":          {"distance": (3000, 12000),  "duree": (20, 70),  "freq_semaine": (2, 4)},
    "Randonnée":       {"distance": (5000, 20000),  "duree": (60, 240), "freq_semaine": (1, 2)},
    "Triathlon":       {"distance": (10000, 40000), "duree": (60, 300), "freq_semaine": (1, 2)},
    "Tennis":          {"distance": None,           "duree": (60, 90),  "freq_semaine": (1, 3)},
    "Natation":        {"distance": None,           "duree": (30, 60),  "freq_semaine": (1, 3)},
    "Football":        {"distance": None,           "duree": (60, 90),  "freq_semaine": (1, 2)},
    "Rugby":           {"distance": None,           "duree": (60, 90),  "freq_semaine": (1, 2)},
    "Badminton":       {"distance": None,           "duree": (45, 90),  "freq_semaine": (1, 2)},
    "Voile":           {"distance": None,           "duree": (120, 240),"freq_semaine": (1, 1)},
    "Judo":            {"distance": None,           "duree": (60, 90),  "freq_semaine": (1, 2)},
    "Boxe":            {"distance": None,           "duree": (45, 90),  "freq_semaine": (1, 2)},
    "Escalade":        {"distance": None,           "duree": (60, 120), "freq_semaine": (1, 2)},
    "Équitation":      {"distance": None,           "duree": (60, 120), "freq_semaine": (1, 2)},
    "Tennis de table": {"distance": None,           "duree": (30, 60),  "freq_semaine": (1, 2)},
    "Basketball":      {"distance": None,           "duree": (60, 90),  "freq_semaine": (1, 2)},
}

# Filet de sécurité si un sport inconnu apparaît un jour dans les données source
DEFAULT_LEISURE_PROFILE = {"distance": None, "duree": (45, 90), "freq_semaine": (1, 2)}

ENGAGEMENT_DECLARE = (0.2, 1.0)      # sport déclaré dans referentiel_sport
ENGAGEMENT_OCCASIONNEL = (0.0, 0.3)  # sport non déclaré, tiré au hasard

WINDOW_DAYS = 365

# Commentaire : présent sur une petite fraction des activités seulement
# (pas un champ purement décoratif à 100% vide, mais pas systématique non plus)
COMMENTAIRE_PROBABILITE = 0.12
COMMENTAIRE_POOL = [
    "RAS", "Belle sortie", "Un peu difficile aujourd'hui", "Beau temps",
    "Avec des collègues", "Séance courte", "Bonne forme",
    "Fatigué(e) mais content(e)", "Sortie en groupe", "Météo pas terrible",
]


def get_connection():
    return psycopg2.connect(
        host=PGHOST, port=PGPORT, dbname=PGDATABASE,
        user=PGUSER, password=PGPASSWORD,
    )


def fetch_employees(cur):
    cur.execute("""
        SELECT r.employee_id, r.date_embauche, r.mode_deplacement_declare, s.sport_pratique
        FROM referentiel_rh r
        JOIN referentiel_sport s ON r.employee_id = s.employee_id
    """)
    return cur.fetchall()


def fetch_sport_distribution(cur):
    """Distribution réelle des sports déclarés, utilisée pour tirer un sport
    plausible chez les non-déclarants (pondéré par la popularité observée,
    pas une distribution uniforme inventée)."""
    cur.execute("""
        SELECT sport_pratique, count(*)
        FROM referentiel_sport
        WHERE sport_pratique IS NOT NULL
        GROUP BY sport_pratique
    """)
    rows = cur.fetchall()
    sports = [r[0] for r in rows]
    weights = [r[1] for r in rows]
    return sports, weights


def assign_effective_sport(sport_declare, sports_pool, weights_pool, rng):
    """Retourne (sport_effectif, engagement, est_declare)."""
    if sport_declare:
        return sport_declare, rng.uniform(*ENGAGEMENT_DECLARE), True
    sport_tire = rng.choices(sports_pool, weights=weights_pool, k=1)[0]
    return sport_tire, rng.uniform(*ENGAGEMENT_OCCASIONNEL), False


def random_time_of_day(rng):
    return timedelta(hours=rng.randint(6, 21), minutes=rng.randint(0, 59))


def maybe_commentaire(rng):
    if rng.random() < COMMENTAIRE_PROBABILITE:
        return rng.choice(COMMENTAIRE_POOL)
    return None


def generate_commute_activities(employee_id, mode, start, end, rng):
    params = COMMUTE_PARAMS.get(mode)
    if not params:
        return []
    activities = []
    current = start
    while current <= end:
        if current.weekday() < 5 and rng.random() < params["prob_jour_ouvre"]:
            distance = rng.randint(*params["distance"])
            duree = rng.randint(*params["duree"])
            debut = datetime.combine(current, datetime.min.time()) + random_time_of_day(rng)
            fin = debut + timedelta(minutes=duree)
            activities.append((employee_id, debut, mode, distance, fin, maybe_commentaire(rng)))
        current += timedelta(days=1)
    return activities


def generate_leisure_activities(employee_id, sport, engagement, start, end, rng):
    """engagement : fraction des semaines où l'activité a effectivement lieu.
    Sans cette variance, un sport pratiqué même au minimum (1x/semaine)
    dépasse mécaniquement le seuil de 15/an (~52/an), ce qui rend le seuil
    non discriminant et casse la démonstration de rejeu d'historique."""
    params = LEISURE_PARAMS.get(sport, DEFAULT_LEISURE_PROFILE)
    activities = []
    week_start = start
    while week_start <= end:
        week_days = [week_start + timedelta(days=i) for i in range(7) if week_start + timedelta(days=i) <= end]
        if rng.random() < engagement:
            n = rng.randint(*params["freq_semaine"])
            chosen = rng.sample(week_days, min(n, len(week_days)))
            for day in chosen:
                distance = rng.randint(*params["distance"]) if params["distance"] else None
                duree = rng.randint(*params["duree"])
                debut = datetime.combine(day, datetime.min.time()) + random_time_of_day(rng)
                fin = debut + timedelta(minutes=duree)
                activities.append((employee_id, debut, sport, distance, fin, maybe_commentaire(rng)))
        week_start += timedelta(days=7)
    return activities


def insert_activities(cur, rows):
    if not rows:
        return 0
    query = """
        INSERT INTO activites_sportives
            (employee_id, date_debut_activite, type_sport, distance_m, date_fin_activite, commentaire)
        VALUES %s
    """
    execute_values(cur, query, rows)
    return len(rows)


def run_historique(conn, seed, force):
    rng = random.Random(seed)
    today = date.today()
    window_start = today - timedelta(days=WINDOW_DAYS)

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM activites_sportives")
        existing = cur.fetchone()[0]
        if existing > 0 and not force:
            print(
                f"activites_sportives contient déjà {existing} lignes -- rien à faire "
                f"(relancer avec --force pour vider et régénérer)."
            )
            return
        if existing > 0 and force:
            cur.execute("TRUNCATE activites_sportives RESTART IDENTITY")

        employees = fetch_employees(cur)
        sports_pool, weights_pool = fetch_sport_distribution(cur)

        all_rows = []
        n_zero_activite = 0
        n_sport_tire = 0
        for employee_id, date_embauche, mode, sport_declare in employees:
            start = max(window_start, date_embauche)

            sport_effectif, engagement, est_declare = assign_effective_sport(
                sport_declare, sports_pool, weights_pool, rng
            )
            if not est_declare:
                n_sport_tire += 1

            rows = []
            rows += generate_commute_activities(employee_id, mode, start, today, rng)
            rows += generate_leisure_activities(employee_id, sport_effectif, engagement, start, today, rng)
            if not rows:
                n_zero_activite += 1
            all_rows.extend(rows)

        n_inserted = insert_activities(cur, all_rows)
    conn.commit()

    n_commentaires = sum(1 for r in all_rows if r[5] is not None)
    print(f"Fenêtre générée : {window_start} -> {today}")
    print(f"Salariés traités : {len(employees)}")
    print(f"  dont sport tiré au hasard (non déclarants) : {n_sport_tire}")
    print(f"Activités insérées : {n_inserted}")
    print(f"  dont avec commentaire : {n_commentaires} ({100 * n_commentaires / n_inserted:.1f}%)")
    print(f"Salariés sans aucune activité générée : {n_zero_activite}")
    print(f"Moyenne par salarié : {n_inserted / len(employees):.1f}")


def run_live(conn, employee_id, count):
    with conn.cursor() as cur:
        if employee_id is None:
            cur.execute("""
                SELECT r.employee_id, r.mode_deplacement_declare, s.sport_pratique
                FROM referentiel_rh r
                JOIN referentiel_sport s ON r.employee_id = s.employee_id
                ORDER BY random() LIMIT 1
            """)
        else:
            cur.execute("""
                SELECT r.employee_id, r.mode_deplacement_declare, s.sport_pratique
                FROM referentiel_rh r
                JOIN referentiel_sport s ON r.employee_id = s.employee_id
                WHERE r.employee_id = %s
            """, (employee_id,))
        row = cur.fetchone()
        if row is None:
            sys.exit("Salarié introuvable.")
        emp_id, mode, sport_declare = row

        rng = random.Random()
        sport_effectif = sport_declare
        if not sport_effectif:
            sports_pool, weights_pool = fetch_sport_distribution(cur)
            sport_effectif = rng.choices(sports_pool, weights=weights_pool, k=1)[0]

        rows = []
        for _ in range(count):
            debut = datetime.now()
            if mode in COMMUTE_PARAMS and rng.random() < 0.5:
                params = COMMUTE_PARAMS[mode]
                distance = rng.randint(*params["distance"])
                duree = rng.randint(*params["duree"])
                fin = debut + timedelta(minutes=duree)
                rows.append((emp_id, debut, mode, distance, fin, maybe_commentaire(rng)))
            else:
                params = LEISURE_PARAMS.get(sport_effectif, DEFAULT_LEISURE_PROFILE)
                distance = rng.randint(*params["distance"]) if params["distance"] else None
                duree = rng.randint(*params["duree"])
                fin = debut + timedelta(minutes=duree)
                rows.append((emp_id, debut, sport_effectif, distance, fin, maybe_commentaire(rng)))

        n_inserted = insert_activities(cur, rows)
    conn.commit()

    repartition = Counter(r[2] for r in rows)
    detail = ", ".join(f"{n}x {type_sport}" for type_sport, n in repartition.items())
    print(f"{n_inserted} activité(s) live insérée(s) pour le salarié {emp_id} : {detail}")


def main():
    parser = argparse.ArgumentParser(description="Générateur d'activités sportives — Sport Data Solution")
    parser.add_argument("--mode", choices=["historique", "live"], required=True)
    parser.add_argument("--seed", type=int, default=42, help="Graine aléatoire (mode historique uniquement)")
    parser.add_argument("--force", action="store_true", help="Vide activites_sportives avant régénération")
    parser.add_argument("--employee-id", type=int, default=None, help="Salarié ciblé (mode live uniquement)")
    parser.add_argument("--live-count", type=int, default=1, help="Nombre d'activités à insérer (mode live)")
    args = parser.parse_args()

    conn = get_connection()
    try:
        if args.mode == "historique":
            run_historique(conn, seed=args.seed, force=args.force)
        else:
            run_live(conn, employee_id=args.employee_id, count=args.live_count)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
