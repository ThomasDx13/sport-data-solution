-- ============================================================
-- Sport Data Solution — POC
-- Couche : Stockage MIROIR (PostgreSQL) — dédiée à PowerBI
-- Alimentée par le job Spark batch (écriture JDBC), en parallèle
-- de l'écriture dans Delta Lake. Modèle en étoile simple.
-- ============================================================

-- ------------------------------------------------------------
-- 1. Dimension salarié (fusion RH + Sport, dénormalisée)
--    Écrasée à chaque run : les attributs RH ne sont pas un axe
--    d'analyse temporel dans ce POC (pas de SCD2 nécessaire ici,
--    contrairement aux paramètres de règles ci-dessous).
-- ------------------------------------------------------------
CREATE TABLE dim_salarie (
    employee_id                 INTEGER PRIMARY KEY,
    nom                          VARCHAR(100) NOT NULL,
    prenom                       VARCHAR(100) NOT NULL,
    bu                           VARCHAR(20) NOT NULL,
    type_contrat                 VARCHAR(10) NOT NULL,
    salaire_brut_annuel          NUMERIC(10,2) NOT NULL,
    mode_deplacement_declare     VARCHAR(50) NOT NULL,
    sport_pratique                 VARCHAR(50),
    date_maj                     TIMESTAMP NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------
-- 2. Dimension paramètres de règles métier — historisée (Type 2)
--    COPIE de parametres_regles (postgres-source), qui est la
--    source de vérité lue par le gold (et par compute_distances.py
--    pour les deux seuils de distance) -- pas l'inverse. Alimentée
--    par le job de synchronisation vers PowerBI, pas seedée ici
--    directement (éviterait deux sources qui peuvent diverger).
--    Permet à PowerBI de montrer explicitement quel taux/seuil
--    s'appliquait à quelle date -> rend le rejeu d'historique
--    visible et justifiable dans le rapport, pas juste recalculé
--    en coulisses.
-- ------------------------------------------------------------
CREATE TABLE dim_parametres_regles (
    parametre_id            SERIAL PRIMARY KEY,
    taux_prime                NUMERIC(5,4) NOT NULL,
    seuil_bien_etre            INTEGER NOT NULL,
    seuil_distance_marche_m    INTEGER NOT NULL,
    seuil_distance_velo_m      INTEGER NOT NULL,
    date_debut_validite        DATE NOT NULL,
    date_fin_validite          DATE,              -- NULL = paramètre actuellement actif
    commentaire                 TEXT
);

-- ------------------------------------------------------------
-- 3. Fait éligibilité et impact financier
--    Grain : un salarié x une exécution de batch (run_date).
--    Miroir direct de la table Delta indicateurs_eligibilite.
--    Les deux avantages sont cumulables (confirmé) -- un salarié
--    peut être éligible aux deux simultanément.
-- ------------------------------------------------------------
CREATE TABLE fait_eligibilite (
    run_date                     TIMESTAMP NOT NULL,   -- horodatage complet, cohérent avec le gold Delta
    employee_id                   INTEGER NOT NULL,   -- pas de FK vers dim_salarie : incompatible avec son TRUNCATE périodique
    nb_activites_12_mois           INTEGER NOT NULL,
    eligible_prime                  BOOLEAN NOT NULL,
    eligible_bien_etre              BOOLEAN NOT NULL,
    nb_jours_bien_etre_accordes     INTEGER NOT NULL,   -- 0 ou 5, lisible sans deviner ce que signifie le booléen
    taux_prime_applique             NUMERIC(5,4) NOT NULL,
    seuil_bien_etre_applique        INTEGER NOT NULL,
    cout_prime                      NUMERIC(10,2) NOT NULL,
    cout_jours_bien_etre            NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (run_date, employee_id)
);

CREATE INDEX idx_fait_eligibilite_date ON fait_eligibilite(run_date);

-- NB : pas de CHECK constraints redondants ici (BU, mode déplacement...) :
-- ce conteneur reçoit des données déjà validées en amont (Great Expectations,
-- côté pipeline). Dupliquer la validation ici ajouterait de la rigidité sans
-- bénéfice, puisque Spark est l'unique rédacteur de cette base.
