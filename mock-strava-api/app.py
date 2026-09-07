"""
mock-strava-api / app.py

Simule l'API Strava (v3) pour le POC Sport Data Solution.

Objectif : remplacer l'appel à une vraie API tierce (impossible à obtenir
pour 161 salariés fictifs) tout en respectant le contrat réel de l'API
Strava (mêmes noms de champs, mêmes mécanismes) pour que le collector
(strava-collector) fasse un vrai travail d'intégration API : pagination,
refresh de token, tolérance aux formats inattendus.

Endpoints exposés (sous-ensemble de l'API Strava réelle) :
  POST /oauth/token               -> authentification (client_credentials
                                      ou refresh_token), renvoie un
                                      access_token à durée de vie limitée.
  GET  /api/v3/athlete/activities -> liste paginée (page, per_page) des
                                      activités "nouvelles" en attente de
                                      collecte. Comportement delta (une
                                      fois servie, une activité n'est plus
                                      proposée) -- cohérent avec l'usage
                                      qu'en fait le collector.

Endpoint hors-scope Strava réel, réservé à la démo de soutenance :
  POST /demo/inject-activities    -> force l'injection immédiate de N
                                      activités dans la file d'attente,
                                      pour rendre visible en direct la
                                      pagination multi-pages et le rejet
                                      de formats invalides sans attendre
                                      le tirage aléatoire naturel (volume
                                      réaliste = faible en continu).

Génération des activités : réutilise les mêmes profils que
generate_activites.py (COMMUTE_PARAMS / LEISURE_PARAMS) pour rester
cohérent avec l'historique déjà chargé -- dupliqué ici plutôt qu'importé,
chaque service ayant son propre contexte de build Docker. Toute évolution
des profils doit être répercutée dans les deux fichiers.

Configuration via variables d'environnement :
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD   (lecture seule des
        référentiels RH/Sport, mêmes noms que le loader)
    STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET            (identifiants
        attendus par /oauth/token)
    MOCK_STRAVA_TOKEN_TTL_SECONDS             (def: 600)
    MOCK_STRAVA_GENERATION_INTERVAL_SECONDS   (def: 180)
    MOCK_STRAVA_MALFORMED_RATE                (def: 0.15)
"""

import asyncio
import os
import random
import secrets
from datetime import datetime, timedelta, timezone

import psycopg2
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
PGHOST = os.getenv("PGHOST", "postgres-source")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "sportdata_source")
PGUSER = os.getenv("PGUSER", "sportdata")
PGPASSWORD = os.getenv("PGPASSWORD", "sportdata_pwd")

CLIENT_ID = os.getenv("STRAVA_CLIENT_ID", "poc_client")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET", "poc_secret")

TOKEN_TTL_SECONDS = int(os.getenv("MOCK_STRAVA_TOKEN_TTL_SECONDS", "600"))
GENERATION_INTERVAL_SECONDS = int(os.getenv("MOCK_STRAVA_GENERATION_INTERVAL_SECONDS", "180"))
MALFORMED_RATE = float(os.getenv("MOCK_STRAVA_MALFORMED_RATE", "0.15"))

# ------------------------------------------------------------------
# Profils de génération -- copie volontaire de generate_activites.py
# (pas d'import cross-service : chaque conteneur a son propre contexte
# de build).
# ------------------------------------------------------------------
COMMUTE_PARAMS = {
    "Marche/running":          {"distance": (1000, 8000),  "duree": (10, 60)},
    "Vélo/Trottinette/Autres": {"distance": (3000, 20000), "duree": (10, 45)},
}

LEISURE_PARAMS = {
    "Runing":          {"distance": (3000, 12000),  "duree": (20, 70)},
    "Randonnée":       {"distance": (5000, 20000),  "duree": (60, 240)},
    "Triathlon":       {"distance": (10000, 40000), "duree": (60, 300)},
    "Tennis":          {"distance": None,           "duree": (60, 90)},
    "Natation":        {"distance": None,           "duree": (30, 60)},
    "Football":        {"distance": None,           "duree": (60, 90)},
    "Rugby":           {"distance": None,           "duree": (60, 90)},
    "Badminton":       {"distance": None,           "duree": (45, 90)},
    "Voile":           {"distance": None,           "duree": (120, 240)},
    "Judo":            {"distance": None,           "duree": (60, 90)},
    "Boxe":            {"distance": None,           "duree": (45, 90)},
    "Escalade":        {"distance": None,           "duree": (60, 120)},
    "Équitation":      {"distance": None,           "duree": (60, 120)},
    "Tennis de table": {"distance": None,           "duree": (30, 60)},
    "Basketball":      {"distance": None,           "duree": (60, 90)},
}
DEFAULT_LEISURE_PROFILE = {"distance": None, "duree": (45, 90)}

COMMENTAIRE_POOL = [
    "RAS", "Belle sortie", "Un peu difficile aujourd'hui", "Beau temps",
    "Avec des collègues", "Séance courte", "Bonne forme",
    "Fatigué(e) mais content(e)", "Sortie en groupe", "Météo pas terrible",
]

# ------------------------------------------------------------------
# État en mémoire (POC -- pas de persistance ; la file repart à vide
# si le conteneur redémarre, acceptable ici)
# ------------------------------------------------------------------
app = FastAPI(title="Mock Strava API — Sport Data Solution POC")

_valid_tokens: dict[str, datetime] = {}       # access_token -> expiry
_refresh_tokens: dict[str, str] = {}          # refresh_token -> access_token courant
_pending_activities: list[dict] = []          # file d'attente FIFO
_activity_seq = 0


def get_connection():
    return psycopg2.connect(
        host=PGHOST, port=PGPORT, dbname=PGDATABASE,
        user=PGUSER, password=PGPASSWORD,
    )


def fetch_employee_pool():
    """Charge les profils salariés depuis postgres-source, comme
    generate_activites.py. Relu à chaque appel : POC, volume faible,
    pas besoin de cache."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT r.employee_id, r.mode_deplacement_declare, s.sport_pratique
                FROM referentiel_rh r
                JOIN referentiel_sport s ON r.employee_id = s.employee_id
            """)
            rows = cur.fetchall()
            cur.execute("""
                SELECT sport_pratique, count(*)
                FROM referentiel_sport
                WHERE sport_pratique IS NOT NULL
                GROUP BY sport_pratique
            """)
            sport_rows = cur.fetchall()
    finally:
        conn.close()
    sports_pool = [r[0] for r in sport_rows]
    weights_pool = [r[1] for r in sport_rows]
    return rows, sports_pool, weights_pool


def build_activity(employee_id, mode, sport_declare, sports_pool, weights_pool, rng):
    """Génère UNE activité 'maintenant', au format Strava (champs
    distance / elapsed_time / start_date) -- équivalent du mode --live
    de generate_activites.py."""
    global _activity_seq
    _activity_seq += 1

    use_commute = mode in COMMUTE_PARAMS and rng.random() < 0.5
    if use_commute:
        params = COMMUTE_PARAMS[mode]
        sport_type = mode
    else:
        sport_effectif = sport_declare or (
            rng.choices(sports_pool, weights=weights_pool, k=1)[0] if sports_pool else "Runing"
        )
        params = LEISURE_PARAMS.get(sport_effectif, DEFAULT_LEISURE_PROFILE)
        sport_type = sport_effectif

    distance = rng.randint(*params["distance"]) if params.get("distance") else None
    elapsed = rng.randint(*params["duree"]) * 60  # minutes -> secondes (comme Strava réel)
    start = datetime.now(timezone.utc)
    comment = rng.choice(COMMENTAIRE_POOL) if rng.random() < 0.12 else None

    return {
        "id": f"mockstrava_{_activity_seq}_{secrets.token_hex(4)}",
        "athlete_id": employee_id,
        "sport_type": sport_type,
        "distance": distance,
        "elapsed_time": elapsed,
        "start_date": start.isoformat().replace("+00:00", "Z"),
        "comment": comment,
    }


def maybe_corrupt(activity: dict, rng: random.Random) -> dict:
    """Avec probabilité MALFORMED_RATE, renvoie une version altérée de
    l'activité (champ manquant ou type incohérent) pour forcer le
    collector à valider ce qu'il reçoit -- comme le ferait un vrai
    tracker tiers avec des données de qualité variable."""
    if rng.random() >= MALFORMED_RATE:
        return activity
    corrupted = dict(activity)
    defect = rng.choice(["missing_field", "bad_type", "null_required"])
    if defect == "missing_field":
        corrupted.pop(rng.choice(["start_date", "athlete_id", "sport_type"]), None)
    elif defect == "bad_type":
        corrupted["distance"] = "beaucoup"  # string au lieu d'un nombre
    elif defect == "null_required":
        corrupted["start_date"] = None
    return corrupted


async def generation_loop():
    """Tâche de fond : simule des salariés qui terminent une activité et
    la synchronisent sur Strava, à intervalle régulier. Volume
    volontairement faible (0 à 2 salariés par tick) -- 161 salariés ne
    génèrent pas un flux massif en continu."""
    rng = random.Random()
    while True:
        await asyncio.sleep(GENERATION_INTERVAL_SECONDS)
        try:
            employees, sports_pool, weights_pool = fetch_employee_pool()
        except Exception as e:
            print(f"[generation_loop] lecture référentiels impossible : {e}")
            continue
        if not employees:
            continue
        n = rng.choices([0, 1, 2], weights=[0.5, 0.35, 0.15], k=1)[0]
        chosen = rng.sample(employees, min(n, len(employees)))
        for employee_id, mode, sport_declare in chosen:
            activity = build_activity(employee_id, mode, sport_declare, sports_pool, weights_pool, rng)
            _pending_activities.append(activity)
        if chosen:
            print(f"[generation_loop] {len(chosen)} activité(s) ajoutée(s) (file en attente : {len(_pending_activities)})")


@app.on_event("startup")
async def on_startup():
    asyncio.create_task(generation_loop())


# ------------------------------------------------------------------
# OAuth (sous-ensemble minimal du vrai flux Strava)
# ------------------------------------------------------------------
@app.post("/oauth/token")
async def oauth_token(request: Request):
    form = await request.form()
    grant_type = form.get("grant_type")
    client_id = form.get("client_id")
    client_secret = form.get("client_secret")

    if client_id != CLIENT_ID or client_secret != CLIENT_SECRET:
        raise HTTPException(status_code=401, detail="invalid client credentials")

    if grant_type == "refresh_token":
        refresh_token = form.get("refresh_token")
        if refresh_token not in _refresh_tokens:
            raise HTTPException(status_code=401, detail="invalid refresh_token")
        old_access = _refresh_tokens.pop(refresh_token)
        _valid_tokens.pop(old_access, None)
    elif grant_type != "client_credentials":
        raise HTTPException(status_code=400, detail="unsupported grant_type")

    access_token = secrets.token_urlsafe(24)
    new_refresh_token = secrets.token_urlsafe(24)
    expiry = datetime.now(timezone.utc) + timedelta(seconds=TOKEN_TTL_SECONDS)

    _valid_tokens[access_token] = expiry
    _refresh_tokens[new_refresh_token] = access_token

    return {
        "token_type": "Bearer",
        "access_token": access_token,
        "refresh_token": new_refresh_token,
        "expires_at": int(expiry.timestamp()),
        "expires_in": TOKEN_TTL_SECONDS,
    }


def _check_token(authorization: str | None):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = authorization.removeprefix("Bearer ")
    expiry = _valid_tokens.get(token)
    if expiry is None or datetime.now(timezone.utc) >= expiry:
        _valid_tokens.pop(token, None)
        raise HTTPException(
            status_code=401,
            detail={
                "message": "Authorization Error",
                "errors": [{"resource": "Athlete", "field": "access_token", "code": "expired"}],
            },
        )


# ------------------------------------------------------------------
# Activités (sous-ensemble de GET /athlete/activities)
# ------------------------------------------------------------------
@app.get("/api/v3/athlete/activities")
async def get_activities(page: int = 1, per_page: int = 30, authorization: str | None = Header(default=None)):
    _check_token(authorization)
    if page < 1 or per_page < 1:
        raise HTTPException(status_code=400, detail="page and per_page must be >= 1")

    rng = random.Random()
    start = (page - 1) * per_page
    end = start + per_page
    slice_ = _pending_activities[start:end]
    result = [maybe_corrupt(a, rng) for a in slice_]

    # Comportement delta : une activité livrée n'est plus reproposée.
    del _pending_activities[start:end]

    return JSONResponse(content=result)


# ------------------------------------------------------------------
# Démo (hors-scope Strava réel) -- injection manuelle pour rendre
# visible en direct la pagination multi-pages et les cas limites,
# sans attendre le tirage aléatoire naturel.
# ------------------------------------------------------------------
@app.post("/demo/inject-activities")
async def demo_inject(count: int = 12):
    employees, sports_pool, weights_pool = fetch_employee_pool()
    if not employees:
        raise HTTPException(status_code=503, detail="référentiels salariés indisponibles")
    rng = random.Random()
    chosen = [rng.choice(employees) for _ in range(count)]
    for employee_id, mode, sport_declare in chosen:
        activity = build_activity(employee_id, mode, sport_declare, sports_pool, weights_pool, rng)
        _pending_activities.append(activity)
    return {"injected": len(chosen), "pending_total": len(_pending_activities)}


@app.get("/health")
async def health():
    return {"status": "ok", "pending": len(_pending_activities)}
