"""
compute_distances.py

Calcule la distance domicile-travail réelle (itinéraire, pas à vol
d'oiseau) pour chaque salarié en mode de trajet "vert", via
OpenRouteService -- alternative gratuite et réelle à Google Maps
(pas de carte bancaire requise, contrairement à Google Maps Platform).

Portée : uniquement les salariés dont mode_deplacement_declare est
Marche/running ou Vélo/Trottinette/Autres -- les autres modes n'ont
aucune règle métier qui utilise cette distance.

Idempotent : un salarié déjà présent dans distances_domicile_travail
n'est jamais recalculé (l'adresse d'un salarié ne change quasiment
jamais -- si elle change un jour, il faudra supprimer sa ligne
manuellement pour forcer un recalcul, pas un besoin courant pour ce POC).

Anomalie : si la distance calculée dépasse la limite raisonnable pour
le mode déclaré, la ligne est quand même insérée (anomalie_distance =
TRUE) -- ça ne fait jamais échouer le job, conformément à la consigne
de faire remonter l'anomalie plutôt que d'interrompre le traitement.
Les seuils par mode sont lus depuis parametres_regles (ligne active),
pas codés en dur ici -- même source de vérité unique que taux_prime /
seuil_bien_etre.

Configuration via variables d'environnement :
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD (mêmes noms que le loader)
    ORS_API_KEY (obligatoire -- inscription gratuite sur openrouteservice.org)
"""

import os
import sys
import time

import openrouteservice
import psycopg2

PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "sportdata_source")
PGUSER = os.getenv("PGUSER", "sportdata")
PGPASSWORD = os.getenv("PGPASSWORD", "sportdata_pwd")

ORS_API_KEY = os.getenv("ORS_API_KEY")

ADRESSE_ENTREPRISE = "1362 Av. des Platanes, 34970 Lattes"

# Profil de routage ORS par mode déclaré. Le seuil de cohérence n'est
# plus ici -- lu depuis parametres_regles via fetch_seuils_actifs().
# Seuls ces deux modes ont une règle métier qui utilise cette distance.
MODE_PARAMS = {
    "Marche/running":          {"profile": "foot-walking"},
    "Vélo/Trottinette/Autres": {"profile": "cycling-regular"},
}

# Délai entre deux salariés traités, pour rester sous la limite de
# 40 appels/minute du plan gratuit ORS (2 appels par salarié : géocodage
# + itinéraire -> ~30 appels/minute avec cette pause, marge de sécurité).
DELAI_ENTRE_SALARIES_SECONDES = 4


def get_connection():
    return psycopg2.connect(
        host=PGHOST, port=PGPORT, dbname=PGDATABASE,
        user=PGUSER, password=PGPASSWORD,
    )


def fetch_seuils_actifs(cur):
    """Seuils de distance lus depuis parametres_regles (ligne active,
    date_fin_validite IS NULL) -- même principe de source de vérité
    unique que taux_prime / seuil_bien_etre, plus codé en dur ici."""
    cur.execute("""
        SELECT seuil_distance_marche_m, seuil_distance_velo_m
        FROM parametres_regles
        WHERE date_fin_validite IS NULL
    """)
    row = cur.fetchone()
    if row is None:
        sys.exit("Aucun paramètre actif dans parametres_regles (date_fin_validite IS NULL).")
    seuil_marche_m, seuil_velo_m = row
    return {"Marche/running": seuil_marche_m, "Vélo/Trottinette/Autres": seuil_velo_m}


def geocode(client, adresse):
    """Retourne (longitude, latitude) pour une adresse texte, ou None si
    aucun résultat -- ne lève pas d'exception pour une adresse introuvable,
    c'est un cas attendu, pas une panne."""
    result = client.pelias_search(text=adresse, size=1)
    features = result.get("features", [])
    if not features:
        return None
    lon, lat = features[0]["geometry"]["coordinates"]
    return (lon, lat)


def compute_route_distance_m(client, coord_origine, coord_destination, profile):
    """Distance d'itinéraire réelle en mètres (pas à vol d'oiseau),
    selon le profil de déplacement (foot-walking, cycling-regular)."""
    routes = client.directions(
        coordinates=[coord_origine, coord_destination],
        profile=profile,
    )
    return round(routes["routes"][0]["summary"]["distance"])


def fetch_employees_a_calculer(cur):
    cur.execute("""
        SELECT r.employee_id, r.adresse, r.mode_deplacement_declare
        FROM referentiel_rh r
        LEFT JOIN distances_domicile_travail d ON r.employee_id = d.employee_id
        WHERE r.mode_deplacement_declare IN %s
          AND d.employee_id IS NULL
        ORDER BY r.employee_id
    """, (tuple(MODE_PARAMS.keys()),))
    return cur.fetchall()


def upsert_distance(cur, employee_id, distance_m, anomalie):
    cur.execute("""
        INSERT INTO distances_domicile_travail
            (employee_id, distance_domicile_travail_m, anomalie_distance)
        VALUES (%s, %s, %s)
        ON CONFLICT (employee_id) DO UPDATE SET
            distance_domicile_travail_m = EXCLUDED.distance_domicile_travail_m,
            anomalie_distance = EXCLUDED.anomalie_distance,
            date_calcul = now()
    """, (employee_id, distance_m, anomalie))


def main():
    if not ORS_API_KEY:
        sys.exit(
            "ORS_API_KEY non défini. Inscription gratuite sur "
            "https://openrouteservice.org/dev/#/signup, puis générer un token."
        )

    client = openrouteservice.Client(key=ORS_API_KEY)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            seuils = fetch_seuils_actifs(cur)
            employees = fetch_employees_a_calculer(cur)

            if not employees:
                print("Aucun salarié à traiter -- tous les modes verts ont déjà une distance calculée.")
                return

            print(f"{len(employees)} salarié(s) à traiter (prévoir plusieurs minutes : "
                  f"délai de {DELAI_ENTRE_SALARIES_SECONDES}s entre chacun + temps de réponse réel de l'API).")

            # Adresse entreprise géocodée une seule fois, réutilisée pour tout le monde.
            coord_entreprise = geocode(client, ADRESSE_ENTREPRISE)
            if coord_entreprise is None:
                sys.exit(f"Adresse entreprise introuvable : {ADRESSE_ENTREPRISE}")

            n_ok = 0
            n_anomalies = 0
            n_erreurs = 0

            for employee_id, adresse, mode in employees:
                params = MODE_PARAMS[mode]
                seuil_m = seuils[mode]
                try:
                    coord_salarie = geocode(client, adresse)
                    if coord_salarie is None:
                        print(f"  [{employee_id}] adresse introuvable, ignoré (sera retenté au prochain run) : {adresse}")
                        n_erreurs += 1
                        continue

                    distance_m = compute_route_distance_m(
                        client, coord_salarie, coord_entreprise, params["profile"]
                    )
                    anomalie = distance_m > seuil_m
                    if anomalie:
                        n_anomalies += 1
                        print(f"  [{employee_id}] ANOMALIE : {distance_m} m en {mode} (seuil {seuil_m} m)")

                    upsert_distance(cur, employee_id, distance_m, anomalie)
                    conn.commit()
                    n_ok += 1

                except Exception as e:
                    print(f"  [{employee_id}] erreur ORS, ignoré (sera retenté au prochain run) : {e}")
                    n_erreurs += 1
                    conn.rollback()

                time.sleep(DELAI_ENTRE_SALARIES_SECONDES)

        print(f"\nTraités : {n_ok} | Anomalies : {n_anomalies} | Erreurs (à retenter) : {n_erreurs}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
