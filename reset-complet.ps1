# reset-complet.ps1
#
# Reset complet du projet Sport Data Solution : supprime toutes les
# données (Postgres, Redpanda, Delta, checkpoints) et reconstruit
# tout depuis zéro dans le bon ordre. À lancer depuis la racine du
# projet (là où se trouve docker-compose.yml).
#
# Usage :
#   .\reset-complet.ps1          (demande confirmation avant de tout supprimer)
#   .\reset-complet.ps1 -Force   (pas de confirmation, utile en script)

param(
    [switch]$Force
)

# ------------------------------------------------------------
# Vérifications préalables
# ------------------------------------------------------------
if (-not (Test-Path "docker-compose.yml")) {
    Write-Host "docker-compose.yml introuvable -- lance ce script depuis la racine du projet." -ForegroundColor Red
    exit 1
}

if (-not $Force) {
    Write-Host "Ceci va supprimer TOUTES les données du projet :" -ForegroundColor Yellow
    Write-Host "  - Base PostgreSQL (référentiels, activités, distances)"
    Write-Host "  - Topics Redpanda"
    Write-Host "  - Tables Delta (bronze, silver)"
    Write-Host "  - Checkpoints Spark"
    $confirmation = Read-Host "`nContinuer ? (o/n)"
    if ($confirmation -ne "o") {
        Write-Host "Annulé."
        exit 0
    }
}

# ------------------------------------------------------------
# Fonction utilitaire : exécute une commande, arrête le script si
# elle échoue -- pour ne jamais enchaîner sur un état cassé.
# ------------------------------------------------------------
function Invoke-Step {
    param(
        [Parameter(Mandatory)][string]$Description,
        [Parameter(Mandatory)][scriptblock]$Command
    )
    Write-Host "`n==> $Description" -ForegroundColor Cyan
    & $Command
    if ($LASTEXITCODE -ne 0) {
        Write-Host "`nÉCHEC à l'étape : $Description" -ForegroundColor Red
        Write-Host "Script arrêté -- corrige le problème ci-dessus avant de relancer." -ForegroundColor Red
        exit 1
    }
}

# ------------------------------------------------------------
# 1. Arrêt complet et suppression des volumes
# ------------------------------------------------------------
Invoke-Step "Arrêt et suppression des volumes (Postgres, Redpanda, checkpoints)" {
    docker compose down -v
}

# ------------------------------------------------------------
# 2. Purge du stockage Delta (bind mount, pas supprimé par down -v)
#    Nécessaire pour éviter une collision d'activity_id : les
#    compteurs Postgres repartent de 1 après le down -v, alors que
#    d'anciennes lignes Delta avec les mêmes ID pourraient encore
#    exister sur disque.
# ------------------------------------------------------------
Write-Host "`n==> Purge du stockage Delta" -ForegroundColor Cyan
$deltaFolders = @("activites_brutes", "activites_enrichies")
foreach ($folder in $deltaFolders) {
    $path = "delta-storage\$folder"
    if (Test-Path $path) {
        Remove-Item -Recurse -Force $path
        Write-Host "  Supprimé : $path"
    }
}

# ------------------------------------------------------------
# 3. Redémarrage des services permanents
# ------------------------------------------------------------
Invoke-Step "Démarrage des services (Postgres, Redpanda, Debezium)" {
    docker compose up -d
}

# ------------------------------------------------------------
# 4. Reconstruction de l'image partagée des scripts (loader,
#    générateurs, compute-distances) -- pas de coût réel si rien
#    n'a changé dans scripts/, le cache Docker s'en charge.
# ------------------------------------------------------------
Invoke-Step "Reconstruction de l'image des scripts" {
    docker compose build
}

# ------------------------------------------------------------
# 5. Bootstrap (référentiels + validation + historique + distances)
#    Délégué à bootstrap.ps1 -- mêmes étapes que ce script effectuait
#    ici même avant, extraites pour être rejouables indépendamment
#    sur une base déjà peuplée (voir bootstrap.ps1). Fonctionne aussi
#    bien ici : la base vient d'être vidée, donc chaque étape s'exécute
#    normalement plutôt que de skip quoi que ce soit.
# ------------------------------------------------------------
Invoke-Step "Bootstrap (référentiels, validation, historique, distances)" {
    .\bootstrap.ps1
}

# ------------------------------------------------------------
# 6. Enregistrement du connecteur Debezium
#    Le slot de réplication a été supprimé avec Postgres --
#    réenregistrement obligatoire, pas juste une bonne pratique.
# ------------------------------------------------------------
Invoke-Step "Enregistrement du connecteur Debezium" {
    docker compose --profile tools run --rm connector-register
}

# ------------------------------------------------------------
# 7. Démarrage du streaming bronze
# ------------------------------------------------------------
Invoke-Step "Démarrage du streaming bronze" {
    docker compose up -d --force-recreate spark-streaming-bronze
}

# ------------------------------------------------------------
# 8. Rattrapage de l'historique en silver
# ------------------------------------------------------------
Invoke-Step "Enrichissement silver (rattrapage de l'historique)" {
    docker compose --profile tools run --rm spark-batch-silver --source postgres
}

# ------------------------------------------------------------
# 9. Calcul des indicateurs d'éligibilité (gold)
# ------------------------------------------------------------
Invoke-Step "Calcul des indicateurs d'éligibilité (gold)" {
    docker compose --profile tools run --rm spark-batch-gold
}

# ------------------------------------------------------------
# 10. Synchronisation du miroir PostgreSQL (PowerBI)
# ------------------------------------------------------------
Invoke-Step "Synchronisation du miroir PowerBI" {
    docker compose --profile tools run --rm sync-mirror
}

Write-Host "`n✔ Reset complet terminé avec succès." -ForegroundColor Green
Write-Host "Pour vérifier le flux live de bout en bout :"
Write-Host "  docker compose --profile tools run --rm generateur-live --mode live --employee-id 17757 --live-count 1"
Write-Host "  (attendre ~15 secondes)"
Write-Host "  docker compose --profile tools run --rm spark-batch-silver"
