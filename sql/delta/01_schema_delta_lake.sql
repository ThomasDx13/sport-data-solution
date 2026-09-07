-- ============================================================
-- Sport Data Solution — POC
-- Couche : Stockage ANALYTIQUE (Delta Lake) — architecture
-- médaillon à 3 niveaux (bronze / silver / gold).
--
-- NB : contrairement au schéma PostgreSQL, ce script n'est pas
-- exécutable seul — il nécessite une session Spark (avec le
-- support Delta) pour être joué. On le garde ici comme document
-- de référence, à exécuter quand la couche Spark sera montée.
-- ============================================================

-- ------------------------------------------------------------
-- 1. BRONZE — Activités brutes
--    Grain : un événement Redpanda, sans aucune transformation
--    ni jointure. Alimentée par le job Spark Structured Streaming
--    qui consomme le topic sportdata.public.activites_sportives.
--
--    Champs conformes à la note de cadrage : distance en MÈTRES
--    (pas km), date de fin d'activité (pas une durée), commentaire.
--    La coquille "Runing" -> "Running" n'est PAS corrigée ici —
--    le bronze reflète fidèlement ce qui a été capturé, la
--    correction se fait uniquement en silver.
--
--    Rôle : conserver une trace durable de tout ce qui a transité
--    par le bus, indépendamment de la rétention du topic Redpanda.
--
--    Partitionnée par date d'INGESTION (arrivée dans Spark), pas
--    par date métier — convention bronze standard.
-- ------------------------------------------------------------
CREATE TABLE activites_brutes (
    activity_id            BIGINT,
    employee_id            INT,
    date_debut_activite     TIMESTAMP,
    type_sport              STRING,
    distance_m              INT,       -- NULL si non pertinent (ex. escalade)
    date_fin_activite        TIMESTAMP,
    commentaire              STRING,    -- NULL la plupart du temps
    created_at                TIMESTAMP,
    op                        STRING,    -- opération CDC ("c"=create ; toujours "c" ici, table source append-only)
    kafka_partition           INT,
    kafka_offset              BIGINT,
    ingestion_date            DATE       -- date d'arrivée dans Spark -> partition technique
)
USING DELTA
PARTITIONED BY (ingestion_date)
LOCATION '/opt/delta/analytics/activites_brutes';

-- ------------------------------------------------------------
-- 2. SILVER — Activités enrichies
--    Grain : une ligne par activité sportive, jointe aux
--    référentiels RH et Sport. Alimentée par un job Spark BATCH
--    INCRÉMENTAL (seules les activités bronze pas encore présentes
--    ici sont traitées à chaque run -- pas de recalcul complet, à
--    la différence du gold qui doit tout recalculer pour permettre
--    le rejeu si un taux/seuil change).
--
--    C'est ici, et uniquement ici, que la coquille "Runing" est
--    corrigée en "Running" -- sur type_sport ET sport_pratique,
--    via la même table de correspondance, pour rester cohérent.
--
--    distance_domicile_travail_km : calculée UNE FOIS PAR SALARIÉ
--    (pas par activité) via OpenRouteService, alternative réelle et
--    gratuite à Google Maps (pas de carte bancaire requise) --
--    profils de trajet réels (foot-walking, cycling-regular).
--
--    anomalie_distance : propagée depuis distances_domicile_travail
--    (calculée par compute_distances.py) -- TRUE si la distance
--    dépasse le seuil raisonnable du mode déclaré. N'interrompt
--    jamais le traitement, seulement signalée (cohérent avec le
--    choix fait dans compute_distances.py).
-- ------------------------------------------------------------
CREATE TABLE activites_enrichies (
    activity_id                    BIGINT,
    employee_id                    INT,
    date_debut_activite             TIMESTAMP,
    type_sport                      STRING,     -- coquilles corrigées (ex. "Running")
    distance_m                      INT,
    date_fin_activite                TIMESTAMP,
    commentaire                      STRING,
    nom                               STRING,
    prenom                            STRING,
    salaire_brut_annuel               DECIMAL(10,2),
    mode_deplacement_declare          STRING,
    sport_pratique                    STRING,     -- coquilles corrigées, cohérent avec type_sport
    distance_domicile_travail_km      DECIMAL(6,2),   -- calculée via OpenRouteService, une fois par salarié
    anomalie_distance                  BOOLEAN,         -- distance > seuil du mode déclaré (propagé depuis distances_domicile_travail)
    run_date                          DATE             -- date du batch ayant traité cette ligne (incrémental)
)
USING DELTA
PARTITIONED BY (run_date)
LOCATION '/opt/delta/analytics/activites_enrichies';

-- ------------------------------------------------------------
-- 3. GOLD — Indicateurs d'éligibilité et impact financier
--    Grain : un salarié × une exécution de batch (run_date).
--    Recalcul COMPLET à chaque run (contrairement au silver) :
--    les paramètres appliqués sont figés dans chaque ligne pour
--    garantir la rejouabilité même si la config évolue ensuite.
--    Les deux avantages sont cumulables (confirmé).
-- ------------------------------------------------------------
CREATE TABLE indicateurs_eligibilite (
    run_date                     TIMESTAMP,   -- horodatage complet, pas juste la date : distingue deux runs le même jour
    employee_id                  INT,
    nb_activites_12_mois         INT,
    eligible_prime                BOOLEAN,
    eligible_bien_etre            BOOLEAN,
    nb_jours_bien_etre_accordes   INT,              -- 0 ou 5, lisible sans deviner ce que signifie le booléen
    taux_prime_applique           DECIMAL(5,4),    -- ex. 0.05
    seuil_bien_etre_applique      INT,              -- ex. 15
    cout_prime                    DECIMAL(10,2),
    cout_jours_bien_etre          DECIMAL(10,2)
)
USING DELTA
PARTITIONED BY (run_date)
LOCATION '/opt/delta/analytics/indicateurs_eligibilite';
