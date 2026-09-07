# Sport Data Solution

POC data engineering (projet scolaire, entreprise fictive) : suivi de l'activité
sportive de 161 salariés pour déterminer l'éligibilité à deux avantages —
une prime liée au mode de transport domicile-travail, et des jours de congé
bien-être liés au volume d'activité physique. Adresse entreprise : 1362 Av.
des Platanes, 34970 Lattes. Données RH/Sport réelles fournies (`dataRH.xlsx`,
`dataSport.xlsx`), historique d'activités entièrement simulé.

## Sommaire

1. [Architecture d'ensemble](#1-architecture-d'ensemble)
2. [Structure du dépôt](#2-structure-du-dépôt)
3. [Pipeline, étape par étape](#3-pipeline-étape-par-étape)
4. [Simulation de l'API Strava — cas limites gérés](#4-simulation-de-l'api-strava--cas-limites-gérés)
5. [Orchestration — Airflow](#5-orchestration--airflow)
6. [Qualité des données — Great Expectations](#6-qualité-des-données--great-expectations)
7. [Monitoring et traçabilité](#7-monitoring-et-traçabilité)
8. [Règles métier et paramétrage](#8-règles-métier-et-paramétrage)
9. [Power BI](#9-power-bi)
10. [Notifications Slack](#10-notifications-slack)
11. [Démonstration en soutenance](#11-démonstration-en-soutenance)
12. [Pistes de montée en charge](#12-pistes-de-montée-en-charge)
13. [Hypothèses documentées](#13-hypothèses-documentées)
14. [Démarrage rapide](#14-démarrage-rapide)
15. [Versions des outils](#15-versions-des-outils)

---

## 1. Architecture d'ensemble

Architecture médaillon (Bronze / Silver / Gold) sur Delta Lake, alimentée par
CDC (Debezium + Redpanda) plutôt que par lecture directe de PostgreSQL, pour
découpler l'ingestion brute de la logique métier et permettre un rejeu de
l'historique au-delà de la rétention du topic Kafka.

```
[dataRH.xlsx / dataSport.xlsx]        [Mock API Strava]
          │ (load unique)                    │ (poll continu)
          ▼                                   ▼
    ┌─────────────────────────────────────────────┐
    │         postgres-source (5432)               │
    │  referentiel_rh · referentiel_sport ·         │
    │  activites_sportives · distances_domicile_    │
    │  travail · parametres_regles (SCD2)           │
    └───────────────────┬───────────────────────────┘
                         │ CDC (Debezium)
                         ▼
                    Redpanda (Kafka)
                    │              │
                    ▼              ▼
         spark-streaming-bronze   slack-notifier
                    │
                    ▼
         Delta BRONZE (activites_brutes)
                    │
                    ▼  (+ référentiels + distances, postgres-source)
         spark-batch-silver → Delta SILVER (activites_enrichies)
                    │
                    ▼  (+ parametres_regles, postgres-source)
         spark-batch-gold → Delta GOLD (indicateurs_eligibilite)
                    │
                    ▼
              sync-mirror
                    │
                    ▼
       postgres-mirror (5433) ──► Power BI
```

Orchestré par **Airflow** (`compute-distances → spark-batch-silver →
spark-batch-gold → sync-mirror`), validé à chaque étage par **Great
Expectations**, avec **Delta Lake** en bind mount local (`delta-storage/`,
pas un volume Docker nommé) pour rester lisible depuis l'hôte (DuckDB).

## 2. Structure du dépôt

```
sport-data-solution/
├── docker-compose.yml
├── .env                        (PROJECT_ROOT, FERNET_KEY, AIRFLOW_UID, ORS_API_KEY,
│                                 identifiants et réglages du mock Strava)
├── COMMANDES.txt                (aide-mémoire des commandes, tenu à jour séparément)
├── reset-complet.ps1            (reset complet de l'environnement)
├── bootstrap.ps1                (alternative idempotente à reset-complet.ps1 —
│                                 rejouable sans tout détruire, s'appuie sur le
│                                 comportement "skip" de generate_activites.py)
├── data/                        (dataRH.xlsx, dataSport.xlsx)
├── sql/
│   ├── source/                  (01_schema_source_postgresql.sql, monté dans
│   │                             postgres-source via docker-entrypoint-initdb.d)
│   ├── mirror/                  (schéma du miroir PowerBI)
│   └── delta/
├── scripts/                     (loader, générateurs, compute_distances)
│   ├── load_referentiels.py
│   ├── generate_activites.py
│   └── compute_distances.py
├── mock-strava-api/             (simulation de l'API Strava — voir §4)
│   ├── app.py
│   ├── requirements.txt
│   └── Dockerfile
├── strava-collector/            (client qui interroge le mock — voir §4)
│   ├── collector.py
│   ├── requirements.txt
│   └── Dockerfile
├── spark/                       (streaming_bronze, silver_enrich, gold_eligibilite,
│                                 sync_mirror, corrections.py)
├── great_expectations/          (validate_source.py, validate_silver.py, validate_gold.py)
├── slack-notifier/               (consommateur Kafka permanent, notifications activités)
├── debezium/register-connector.json
├── delta-storage/                (bind mount local, PAS un volume Docker)
├── airflow/                      (Dockerfile, dags/, logs/, config/, plugins/)
└── demo/                         (scripts de démonstration pour la soutenance)
    ├── regle_update.py           (mise à jour SCD2 live d'un paramètre de règle)
    └── trigger_live_activity.py  (injection immédiate d'activités dans le mock Strava)
```

## 3. Pipeline, étape par étape

### 3.1 Référentiels (chargement initial)

`scripts/load_referentiels.py` charge `dataRH.xlsx` / `dataSport.xlsx` dans
`referentiel_rh` / `referentiel_sport` (upsert, idempotent — rejouable sans
duplication). `parametres_regles` est initialisé par le schéma SQL lui-même
(`sql/source/01_schema_source_postgresql.sql`), une ligne active par défaut.

### 3.2 Génération et ingestion des activités

Deux mécanismes distincts, documentés en détail dans le docstring de
`generate_activites.py` :

- **Historique** (`--mode historique`) : génère ~12 mois d'activités
  plausibles pour les 161 salariés, à partir des référentiels déjà chargés
  (pas des `.xlsx`). Idempotent par défaut (skip si la table contient déjà
  des lignes), `--force` pour régénérer volontairement.
- **Live** : depuis l'ajout de la simulation API Strava (§4), le flux
  "live" ne s'insère plus directement en base — il passe par
  `mock-strava-api` → `strava-collector`, pour que le collector fasse un
  vrai travail d'intégration API (auth, pagination, tolérance aux formats
  invalides), conformément au critère d'évaluation correspondant. Le mode
  `--mode live` de `generate_activites.py` reste disponible comme
  raccourci de debug local (insertion directe, sans passer par l'API).

Champs conformes à la note de cadrage : `ID`, `ID salarié`,
`date_debut_activite`, `type`, `distance_m` (**mètres**, pas km),
`date_fin_activite`, `commentaire`. La durée n'est jamais stockée telle
quelle — elle se déduit de `date_fin_activite - date_debut_activite`.

`compute_distances.py` calcule la distance domicile-travail réelle via
**OpenRouteService** (alternative gratuite à Google Maps, qui nécessite une
carte bancaire), une fois par salarié — pas par activité —, uniquement pour
les modes de trajet "verts" (Marche/running, Vélo/Trottinette/Autres).
Anomalie flaguée si la distance dépasse le seuil raisonnable du mode déclaré
(seuils lus dans `parametres_regles`).

### 3.3 CDC — Debezium + Redpanda

Debezium capture uniquement `activites_sportives`
(`snapshot.mode=no_data` — pas de rejeu de l'historique au démarrage). SMT
`unwrap` + `TimestampConverter` pour aplatir l'enveloppe et rendre les
timestamps lisibles ; `decimal.handling.mode=double`.

### 3.4 Spark + Delta Lake — architecture médaillon

- **Bronze** (`spark-streaming-bronze`, service permanent) : `activites_brutes`,
  consomme Redpanda en continu structured streaming.
- **Silver** (`spark-batch-silver`, batch **incrémental**) : `activites_enrichies`
  — jointure référentiels + distances, correction de la coquille
  "Runing" → "Running" (module partagé `corrections.py`). `--source bronze`
  (flux continu, défaut) ou `--source postgres` (rattrapage de l'historique
  généré en masse par `generateur-historique`).
- **Gold** (`spark-batch-gold`, batch, **recalcul complet** à chaque run) :
  `indicateurs_eligibilite` — éligibilité prime/bien-être (cumulables),
  coûts associés. `run_date` en **TIMESTAMP** (pas `DATE`, pour distinguer
  plusieurs runs le même jour). Recalcul complet plutôt qu'incrémental :
  nécessaire pour permettre le rejeu si un taux/seuil change dans
  `parametres_regles`.

### 3.5 Miroir PostgreSQL (Power BI)

`postgres-mirror` (port 5433), alimenté par le job `sync-mirror` :
`dim_salarie` (écrasement complet), `dim_parametres_regles` (écrasement,
copie de `parametres_regles` — **le miroir est une copie, jamais la
source de vérité**), `fait_eligibilite` (incrémental par `run_date`, pas de
FK vers `dim_salarie`, incompatible avec son TRUNCATE périodique).

## 4. Simulation de l'API Strava — cas limites gérés

Dans un contexte réel, les activités des salariés seraient récupérées via
l'API Strava de chaque utilisateur. Pour ce POC, la récupération est
simulée par deux services permanents plutôt que par une écriture directe
en base, afin que le connecteur fasse un vrai travail d'intégration API :

- **`mock-strava-api`** génère des activités plausibles en tâche de fond
  (réutilisant les profils de `generate_activites.py`) et les expose via un
  sous-ensemble réaliste de l'[API Strava v3](https://developers.strava.com/docs/reference/)
  (mêmes noms de champs : `distance` en mètres, `elapsed_time` en secondes,
  `start_date` en ISO 8601 ; mêmes paramètres de pagination `page`/`per_page`).
- **`strava-collector`** interroge ce mock en continu, exactement comme le
  ferait un connecteur d'entreprise réel, et écrit les activités valides
  dans `activites_sportives`. Debezium capte l'insert comme n'importe
  quelle autre écriture — rien ne change en aval.

Cas limites explicitement gérés :

| Cas | Où | Comportement |
|---|---|---|
| **Pagination** | `strava-collector.poll_once()` | Boucle `page += 1` jusqu'à réception d'une page vide. `STRAVA_COLLECTOR_PER_PAGE` (déf. 5) volontairement petit pour que la pagination s'exerce dès qu'une file dépasse quelques activités. |
| **Token expiré** | `strava-collector.TokenManager` | Le token a une durée de vie limitée (`MOCK_STRAVA_TOKEN_TTL_SECONDS`, déf. 600s). Le `TokenManager` refresh proactivement avant expiration ; en complément, un `401` reçu en plein cycle déclenche un refresh forcé puis un nouvel essai (une seule fois, pour éviter une boucle infinie en cas de panne réelle de l'API). |
| **Format inattendu** | `mock-strava-api.maybe_corrupt()` / `strava-collector.validate_activity()` | Le mock altère aléatoirement une fraction des activités servies (`MOCK_STRAVA_MALFORMED_RATE`, déf. 0.15) : champ manquant, type incohérent, `start_date` nulle. Le collector valide chaque activité reçue avant insertion ; une activité invalide est rejetée et loggée, jamais insérée, et n'interrompt pas le cycle. |
| **Absence de données** | `strava-collector.poll_once()` | Le volume réaliste (161 salariés) est naturellement faible : la plupart des cycles de poll ne trouvent rien. Une file vide en page 1 termine le cycle sans erreur — cas déjà couvert nativement, pas besoin d'un artifice supplémentaire. |

Paramètres de collecte externalisés (`.env`, jamais en dur dans le code) :
`STRAVA_API_BASE_URL`, `STRAVA_CLIENT_ID`, `STRAVA_CLIENT_SECRET`,
`MOCK_STRAVA_TOKEN_TTL_SECONDS`, `MOCK_STRAVA_GENERATION_INTERVAL_SECONDS`,
`MOCK_STRAVA_MALFORMED_RATE`, `STRAVA_COLLECTOR_POLL_INTERVAL_SECONDS`,
`STRAVA_COLLECTOR_PER_PAGE`.

**Choix de configuration** : le TTL du token (10 min) et l'intervalle de
poll (60s) ont été fixés à des valeurs réalistes plutôt qu'artificiellement
courtes — le refresh de token n'a pas besoin d'être visible en direct
pendant la soutenance pour être évalué (ce critère est vérifié via le code
et ce README, pas par démonstration live). Le volume d'activités générées
en tâche de fond reste volontairement faible et réaliste (0 à 2 salariés
toutes les 3 minutes) ; pour rendre la pagination et le rejet de formats
invalides visibles à la demande, voir `demo/trigger_live_activity.py`
(§11).

## 5. Orchestration — Airflow

DAG `sportdata_pipeline` : `compute-distances → spark-batch-silver →
spark-batch-gold → sync-mirror`. Planifié quotidien, **pausé par défaut**,
déclenché manuellement pour la démo. Chaque tâche exécute
`docker compose run` depuis le conteneur scheduler lui-même (socket Docker
monté, projet monté au même chemin absolu WSL2 des deux côtés,
`${PROJECT_ROOT}`).

`mock-strava-api` et `strava-collector` ne font **pas** partie de ce DAG :
ce sont des services permanents (comme `spark-streaming-bronze` ou
`slack-notifier`), pas des jobs planifiés — l'ingestion live est continue
par nature, indépendante du cycle batch quotidien.

## 6. Qualité des données — Great Expectations

Validation à chaque étage :

- **`validate_source.py`** — moteur SQL (PostgreSQL) : intégrité
  référentielle sur `referentiel_rh`/`referentiel_sport`, invariants SCD2
  sur `parametres_regles` (`UnexpectedRowsExpectation`).
- **`validate_silver.py`** / **`validate_gold.py`** — moteur pandas, lecture
  Delta directe via `deltalake` (pas de moteur Spark/JVM pour ces
  validations, plus léger).

Distinction `severity="warning"` (signale sans bloquer, ex. `anomalie_distance`)
vs `"critical"` (bloque le pipeline) — **vérifiée empiriquement** par
manipulation volontaire de données pour confirmer le comportement réel des
deux niveaux, pas seulement supposée d'après la documentation.

## 7. Monitoring et traçabilité

Pas de stack Grafana/Prometheus dédiée pour ce POC — le guide d'évaluation
cite explicitement Kestra (un orchestrateur) comme exemple d'outil de
monitoring acceptable, ce qui indique que l'UI native d'un orchestrateur
suffit. L'ensemble des critères (succès/échec, temps d'exécution,
volumétrie, alertes, seuils) est couvert par la combinaison suivante,
déjà en place plutôt qu'ajoutée pour la forme :

- **État succès/échec + temps d'exécution** : UI Airflow native (vue
  Graph, durées de tâche).
- **Alertes** : `slack-notifier` (notification par activité captée) +
  alertes d'échec Airflow.
- **Seuils** : distinction `severity` de Great Expectations (§6).

## 8. Règles métier et paramétrage

`parametres_regles` (`postgres-source`) est la **source de vérité**,
historisée en **SCD2** (une ligne par période de validité,
`date_fin_validite IS NULL` = ligne active). Le gold lit la ligne active à
chaque run et la fige dans chaque ligne produite (`run_date` capture le
taux/seuil appliqué à ce moment-là — traçabilité complète en cas de
changement de règle).

- **Prime** : mode de trajet vert + distance domicile-travail cohérente
  avec le mode déclaré (seuils `seuil_distance_marche_m` /
  `seuil_distance_velo_m`).
- **Jours bien-être** : ≥ `seuil_bien_etre` activités sur les 12 derniers
  mois glissants.
- Les deux avantages sont **cumulables**.

`demo/regle_update.py` permet de démontrer en direct une mise à jour
conforme SCD2 d'un paramètre (voir §11).

## 9. Power BI

Rapport connecté à `postgres-mirror` : segment `run_date` (style *List*,
pas *Between*), cartes KPI avec pourcentage de contexte, graphique de
comparaison entre runs (axe catégoriel, pas continu), table d'historique
des paramètres avec surbrillance de la ligne active (mesure DAX comparant
les périodes de validité au `run_date` sélectionné), répartition par BU.

## 10. Notifications Slack

`slack-notifier` est un consommateur Kafka **permanent**, distinct du
canal d'alertes d'échec Airflow : il lit
`sportdata.public.activites_sportives` et poste une notification pour
chaque nouvelle activité captée (nom du salarié résolu via
`referentiel_rh`, catégorisation trajet/loisir).

## 11. Démonstration en soutenance

Deux scripts dans `demo/`, tous deux pensés pour rendre visible en direct
un mécanisme qui, sinon, ne se déclencherait qu'au hasard ou lentement :

- **`regle_update.py`** — met à jour un paramètre de `parametres_regles`
  en respectant l'invariant SCD2 (clôture de la ligne active, insertion de
  la nouvelle), pour démontrer le rejeu du gold avec un nouveau taux/seuil.
- **`trigger_live_activity.py`** — force `mock-strava-api` à injecter
  immédiatement plusieurs activités (`--count`), pour rendre visible en
  quelques minutes une vraie pagination multi-pages côté
  `strava-collector` et le rejet d'activités malformées, sans attendre le
  tirage aléatoire naturel du générateur en tâche de fond.

## 12. Pistes de montée en charge

- **Plus de données** : le split Bronze/Silver/Gold découple déjà
  l'ingestion brute de la logique métier — augmenter le volume ne change
  rien à l'architecture, seulement le dimensionnement (partitionnement des
  tables Delta par date/BU, plusieurs partitions Redpanda, passage
  d'Airflow en `CeleryExecutor`/`KubernetesExecutor` plutôt que
  `LocalExecutor`). Le gold pourrait aussi passer d'un recalcul complet à
  un `MERGE` incrémental limité aux salariés ou périodes réellement
  affectés par un changement de règle, si le volume le justifiait.
- **Autre source** : `strava-collector` illustre déjà le patron à suivre —
  un nouveau connecteur (autre tracker, badgeuse, etc.) n'a qu'à écrire
  dans `activites_sportives` (ou produire directement sur Redpanda) en
  respectant le contrat de champs ; aucune modification requise en Silver/Gold
  tant que le schéma est respecté.
- **Autres KPI** : le patron paramètres versionnés en SCD2
  (`parametres_regles`) + gold en recalcul complet est réutilisable tel
  quel pour un nouvel indicateur — un nouveau script gold lisant les mêmes
  Bronze/Silver + référentiels suffit, sans toucher à l'ingestion.

## 13. Hypothèses documentées

| Hypothèse | Justification |
|---|---|
| Coût d'un jour de congé bien-être = `salaire_brut_annuel / 218` jours travaillés/an | Valeur standard française à défaut d'indication contraire dans le cadrage |
| Les deux avantages sont cumulables | Rien dans le cadrage ne l'interdit |
| Distance domicile-travail calculée une fois par salarié, modes verts uniquement | Cohérent avec l'usage qu'en fait la règle prime |
| Gestion des départs de salariés | Explicitement hors périmètre du POC |
| Le "live" est une simulation d'API tierce, pas un vrai flux Strava | Impossible d'obtenir de vrais comptes Strava pour 161 salariés fictifs |

## 14. Démarrage rapide

```bash
# Setup complet (référentiels + historique + connecteur CDC)
# voir bootstrap.ps1 (idempotent) ou reset-complet.ps1 (reset total)

# Services permanents
docker compose up -d postgres-source postgres-mirror redpanda
docker compose --profile tools run --rm loader
docker compose --profile tools run --rm generateur-historique
docker compose up -d spark-streaming-bronze mock-strava-api strava-collector slack-notifier
docker compose --profile tools run --rm connector-register

# Orchestration
docker compose up -d airflow-postgres airflow-init airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer
# UI Airflow : http://localhost:8081 — dépausser sportdata_pipeline manuellement
```

Détail des commandes courantes : voir `COMMANDES.txt`.

## 15. Versions des outils

Spark 3.5.5 (Scala 2.12, Java 11) · Delta 3.3.2 · JDBC PostgreSQL 42.7.13 ·
Airflow 3.3.1 · Redpanda v26.2.1 · Debezium `quay.io/debezium/connect:latest` ·
Great Expectations 1.21 · FastAPI 0.115.0 (mock-strava-api)
