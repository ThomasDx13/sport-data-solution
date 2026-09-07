"""
strava-collector / collector.py

Client qui simule un connecteur Strava réel côté entreprise : interroge
en continu mock-strava-api, gère l'authentification OAuth (refresh de
token), pagine jusqu'à épuisement, valide chaque activité reçue et écrit
les activités valides dans postgres-source (activites_sportives).
Debezium capte ensuite l'insert comme n'importe quelle autre écriture
dans cette table -- aucune modification en aval.

Cas limites gérés (cf. critère CE2 - Compétence 2) :
  - pagination         : boucle jusqu'à page vide
  - token expiré        : 401 -> refresh -> nouvel essai (une fois)
  - format inattendu    : champ manquant / type incohérent -> skip + log
  - absence de données  : file vide côté API -> cycle sans effet, pas d'erreur

Configuration via variables d'environnement :
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD   (écriture, mêmes
        noms que le loader / generate_activites.py)
    STRAVA_API_BASE_URL                     (def: http://mock-strava-api:8000)
    STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET
    STRAVA_COLLECTOR_POLL_INTERVAL_SECONDS  (def: 60)
    STRAVA_COLLECTOR_PER_PAGE               (def: 5)
"""

import logging
import os
import time
from datetime import datetime, timedelta, timezone

import psycopg2
import requests
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("strava-collector")

PGHOST = os.getenv("PGHOST", "postgres-source")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "sportdata_source")
PGUSER = os.getenv("PGUSER", "sportdata")
PGPASSWORD = os.getenv("PGPASSWORD", "sportdata_pwd")

API_BASE_URL = os.getenv("STRAVA_API_BASE_URL", "http://mock-strava-api:8000")
CLIENT_ID = os.getenv("STRAVA_CLIENT_ID", "poc_client")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET", "poc_secret")

POLL_INTERVAL_SECONDS = int(os.getenv("STRAVA_COLLECTOR_POLL_INTERVAL_SECONDS", "60"))
PER_PAGE = int(os.getenv("STRAVA_COLLECTOR_PER_PAGE", "5"))

REQUIRED_FIELDS = {"id", "athlete_id", "sport_type", "start_date", "elapsed_time"}


def get_connection():
    return psycopg2.connect(
        host=PGHOST, port=PGPORT, dbname=PGDATABASE,
        user=PGUSER, password=PGPASSWORD,
    )


class TokenManager:
    """Gère l'obtention et le refresh du token OAuth. Ne redemande un
    nouveau token que si celui en mémoire est absent ou expiré -- évite
    un appel /oauth/token à chaque poll (le TTL, 10 min par défaut, est
    largement supérieur à l'intervalle de poll)."""

    def __init__(self):
        self.access_token = None
        self.refresh_token = None
        self.expires_at = datetime.min.replace(tzinfo=timezone.utc)

    def _request_token(self, grant_type, refresh_token=None):
        payload = {"grant_type": grant_type, "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}
        if refresh_token:
            payload["refresh_token"] = refresh_token
        resp = requests.post(f"{API_BASE_URL}/oauth/token", data=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        self.access_token = data["access_token"]
        self.refresh_token = data["refresh_token"]
        self.expires_at = datetime.fromtimestamp(data["expires_at"], tz=timezone.utc)
        log.info("Token obtenu, expire à %s", self.expires_at.isoformat())

    def get_valid_token(self):
        if self.access_token is None:
            self._request_token("client_credentials")
        elif datetime.now(timezone.utc) >= self.expires_at - timedelta(seconds=5):
            log.info("Token expiré ou sur le point de l'être -- refresh")
            self._request_token("refresh_token", refresh_token=self.refresh_token)
        return self.access_token

    def force_refresh(self):
        """Appelé après un 401 inattendu en plein cycle (ex. token
        révoqué côté API) : on ignore l'état en mémoire et on redemande
        un token neuf plutôt qu'un refresh, par prudence."""
        self._request_token("client_credentials")


def validate_activity(raw: dict) -> tuple[bool, str | None]:
    """Vérifie qu'une activité reçue est exploitable. Retourne
    (est_valide, raison_du_rejet le cas échéant)."""
    missing = REQUIRED_FIELDS - raw.keys()
    if missing:
        return False, f"champs manquants : {sorted(missing)}"
    if raw.get("start_date") is None:
        return False, "start_date est null"
    if not isinstance(raw.get("athlete_id"), int):
        return False, "athlete_id n'est pas un entier"
    distance = raw.get("distance")
    if distance is not None and not isinstance(distance, (int, float)):
        return False, f"distance de type inattendu : {type(distance).__name__}"
    elapsed = raw.get("elapsed_time")
    if not isinstance(elapsed, (int, float)) or elapsed <= 0:
        return False, "elapsed_time invalide"
    try:
        datetime.fromisoformat(raw["start_date"].replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return False, "start_date non parsable"
    return True, None


def to_row(raw: dict) -> tuple:
    """Convertit une activité au format Strava en ligne prête pour
    activites_sportives. date_fin_activite est calculée à partir de
    elapsed_time -- la durée n'est jamais stockée telle quelle,
    conformément à la note de cadrage."""
    start = datetime.fromisoformat(raw["start_date"].replace("Z", "+00:00"))
    end = start + timedelta(seconds=raw["elapsed_time"])
    return (
        raw["athlete_id"],
        start,
        raw["sport_type"],
        raw.get("distance"),
        end,
        raw.get("comment"),
    )


def insert_rows(rows: list[tuple]) -> int:
    if not rows:
        return 0
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO activites_sportives
                    (employee_id, date_debut_activite, type_sport, distance_m, date_fin_activite, commentaire)
                VALUES %s
            """, rows)
        conn.commit()
    finally:
        conn.close()
    return len(rows)


def fetch_page(token_manager: TokenManager, page: int, retried: bool = False) -> list[dict]:
    token = token_manager.get_valid_token()
    resp = requests.get(
        f"{API_BASE_URL}/api/v3/athlete/activities",
        params={"page": page, "per_page": PER_PAGE},
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    if resp.status_code == 401:
        if retried:
            raise RuntimeError("401 persistant après refresh -- abandon du cycle")
        log.warning("401 reçu en plein cycle -- refresh forcé et nouvel essai")
        token_manager.force_refresh()
        return fetch_page(token_manager, page, retried=True)
    resp.raise_for_status()
    return resp.json()


def poll_once(token_manager: TokenManager):
    page = 1
    n_valid = 0
    n_rejected = 0
    rows = []

    while True:
        activities = fetch_page(token_manager, page)
        if not activities:
            break  # dernière page (ou file vide -- cas "absence de données")
        for raw in activities:
            ok, reason = validate_activity(raw)
            if ok:
                rows.append(to_row(raw))
                n_valid += 1
            else:
                n_rejected += 1
                log.warning("Activité rejetée (%s) : %s", reason, raw)
        page += 1

    n_inserted = insert_rows(rows)
    if n_valid or n_rejected:
        log.info("Cycle terminé : %d insérée(s), %d rejetée(s), %d page(s) parcourue(s)", n_inserted, n_rejected, page - 1)
    else:
        log.info("Cycle terminé : aucune activité en attente")


def main():
    log.info("strava-collector démarré -- poll toutes les %ds, %d activités/page", POLL_INTERVAL_SECONDS, PER_PAGE)
    token_manager = TokenManager()
    while True:
        try:
            poll_once(token_manager)
        except Exception:
            log.exception("Erreur durant le cycle de poll -- on continue au prochain tick")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
