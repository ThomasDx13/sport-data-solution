-- ============================================================
-- Sport Data Solution — POC
-- Couche : Stockage SOURCE (PostgreSQL)
-- Contenu : référentiels statiques + table CDC des activités
-- Schéma vérifié contre dataRH.xlsx / dataSport.xlsx (161 lignes)
-- ============================================================

-- ------------------------------------------------------------
-- 1. Référentiel RH (issu de dataRH.xlsx)
--    Chargé tel quel, sert de racine pour les jointures.
-- ------------------------------------------------------------
CREATE TABLE referentiel_rh (
    employee_id                 INTEGER PRIMARY KEY,       -- "ID salarié", entier, unique confirmé
    nom                          VARCHAR(100) NOT NULL,
    prenom                       VARCHAR(100) NOT NULL,
    date_naissance               DATE NOT NULL,
    bu                           VARCHAR(20) NOT NULL
        CHECK (bu IN ('Marketing', 'R&D', 'Ventes', 'Support', 'Finance')),
    date_embauche                DATE NOT NULL,
    salaire_brut_annuel          NUMERIC(10,2) NOT NULL,    -- hypothèse : "Salaire brut" = annuel (cohérent avec le brief "prime de 5% du salaire annuel brut")
    type_contrat                 VARCHAR(10) NOT NULL
        CHECK (type_contrat IN ('CDI', 'CDD')),
    nombre_jours_cp              INTEGER NOT NULL,
    adresse                      TEXT NOT NULL,              -- utilisée ensuite par l'appel Google Maps
    mode_deplacement_declare     VARCHAR(50) NOT NULL
        CHECK (mode_deplacement_declare IN
            ('Transports en commun', 'véhicule thermique/électrique',
             'Marche/running', 'Vélo/Trottinette/Autres')),
    date_maj                     TIMESTAMP NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------
-- 2. Référentiel Sport (issu de dataSport.xlsx)
--    Remplissage partiel confirmé : 95/161 lignes renseignées.
--    Hypothèse à trancher : que fait-on d'un salarié sans sport
--    déclaré (66/161) ? (ex. déduit de l'activité générée, ou
--    catégorie "non renseigné"). À documenter.
-- ------------------------------------------------------------
CREATE TABLE referentiel_sport (
    employee_id   INTEGER PRIMARY KEY
                      REFERENCES referentiel_rh(employee_id),
    sport_pratique  VARCHAR(50),   -- NULL si non déclaré ; colonne source "Pratique d'un sport"
    date_maj      TIMESTAMP NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------
-- 3. Activités sportives simulées — TABLE SOURCE DU CDC
--    Alimentée par le générateur Faker (~12 mois d'historique
--    + insertions ponctuelles pour la démo live).
--    Conçue append-only : une activité, une fois enregistrée,
--    n'est ni modifiée ni supprimée (comme sur un vrai tracker).
--
--    Champs conformes à la note de cadrage : ID, ID salarié, date
--    de début, type, distance (mètres, NULL si non pertinent),
--    date de fin, commentaire. La durée n'est pas stockée telle
--    quelle -- elle se déduit de (date_fin - date_debut).
-- ------------------------------------------------------------
CREATE TABLE activites_sportives (
    activity_id           BIGSERIAL PRIMARY KEY,
    employee_id           INTEGER NOT NULL
                               REFERENCES referentiel_rh(employee_id),
    date_debut_activite    TIMESTAMP NOT NULL,
    type_sport             VARCHAR(50) NOT NULL,
    distance_m             INTEGER,             -- NULL si non pertinent (ex. escalade)
    date_fin_activite       TIMESTAMP NOT NULL,
    commentaire             TEXT,                -- NULL la plupart du temps (~85-90%)
    created_at              TIMESTAMP NOT NULL DEFAULT now()   -- utile pour audit + CDC
);

CREATE INDEX idx_activites_employee ON activites_sportives(employee_id);
CREATE INDEX idx_activites_date     ON activites_sportives(date_debut_activite);

-- ------------------------------------------------------------
-- 4. Pré-requis techniques pour Debezium (CDC)
-- ------------------------------------------------------------
-- REPLICA IDENTITY ne concerne que les UPDATE/DELETE (aucun effet
-- sur les INSERT). Cette table étant append-only, le réglage
-- DEFAULT (basé sur la clé primaire) suffit -> aucune ALTER TABLE.
--
-- Au niveau serveur (postgresql.conf, configuré via docker-compose) :
--    wal_level = logical
--    -> sans ça, Debezium ne peut pas se connecter en réplication logique.

-- ------------------------------------------------------------
-- 5. Distances domicile-travail — CALCULÉES, pas chargées depuis
--    un fichier source. Une ligne par salarié en mode de trajet
--    "vert" (Marche/running, Vélo/Trottinette/Autres) uniquement --
--    les autres modes n'ont aucune règle qui utilise cette distance.
--    Peuplée par scripts/compute_distances.py (OpenRouteService),
--    pas par le loader. Lue par le job silver (Spark), pas par le
--    bus CDC -- ce n'est pas une table source d'activité.
-- ------------------------------------------------------------
CREATE TABLE distances_domicile_travail (
    employee_id                    INTEGER PRIMARY KEY
                                        REFERENCES referentiel_rh(employee_id),
    distance_domicile_travail_m     INTEGER NOT NULL,
    anomalie_distance                BOOLEAN NOT NULL,   -- distance > limite raisonnable du mode déclaré
    date_calcul                      TIMESTAMP NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------
-- 6. Paramètres de règles métier — SOURCE DE VÉRITÉ pour le gold
--    (et pour compute_distances.py, pour les deux seuils de
--    distance ci-dessous).
--    Historisée en Type 2 (une ligne par période de validité) :
--    permet de savoir quel taux/seuil s'appliquait à quelle date.
--    Éditable directement ici pour changer le taux/seuil futur --
--    le gold lit la ligne active (date_fin_validite IS NULL) à
--    chaque run et la fige dans chaque ligne produite.
--    Le miroir PowerBI (dim_parametres_regles) est une COPIE de
--    cette table, alimentée plus tard par le job de synchronisation --
--    pas l'inverse.
-- ------------------------------------------------------------
CREATE TABLE parametres_regles (
    parametre_id              SERIAL PRIMARY KEY,
    taux_prime                  NUMERIC(5,4) NOT NULL,
    seuil_bien_etre              INTEGER NOT NULL,
    seuil_distance_marche_m      INTEGER NOT NULL,   -- limite raisonnable à pied avant anomalie (lu par compute_distances.py)
    seuil_distance_velo_m        INTEGER NOT NULL,   -- limite raisonnable à vélo avant anomalie (lu par compute_distances.py)
    date_debut_validite          DATE NOT NULL,
    date_fin_validite            DATE,              -- NULL = paramètre actuellement actif
    commentaire                   TEXT
);

INSERT INTO parametres_regles
    (taux_prime, seuil_bien_etre, seuil_distance_marche_m, seuil_distance_velo_m,
     date_debut_validite, date_fin_validite, commentaire)
VALUES
    (0.05, 15, 15000, 25000, '2026-01-01', NULL, 'Valeurs initiales retenues pour le POC');
