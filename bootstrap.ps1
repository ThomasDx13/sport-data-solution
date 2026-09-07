# bootstrap.ps1
#
# Charge/complète les référentiels, valide leur qualité, génère
# l'historique d'activités et calcule les distances domicile-travail --
# REJOUABLE SANS RISQUE sur une base déjà peuplée : chaque étape est
# idempotente (upsert pour le loader, skip si déjà peuplé pour
# generateur-historique, skip les salariés déjà calculés pour
# compute-distances). Ne supprime jamais rien.
#
# Pour repartir de zéro (tout supprimer puis rebootstrapper), voir
# reset-complet.ps1, qui appelle ce script après avoir vidé la base.
#
# Usage :
#   .\bootstrap.ps1

# ------------------------------------------------------------
# Vérifications préalables
# ------------------------------------------------------------
if (-not (Test-Path "docker-compose.yml")) {
    Write-Host "docker-compose.yml introuvable -- lance ce script depuis la racine du projet." -ForegroundColor Red
    exit 1
}

# ------------------------------------------------------------
# Fonction utilitaire : exécute une commande, arrête le script si
# elle échoue -- pour ne jamais enchaîner sur un état cassé.
# (identique à reset-complet.ps1)
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
# 1. Chargement des référentiels (idempotent : upsert)
# ------------------------------------------------------------
Invoke-Step "Chargement des référentiels RH/Sport" {
    docker compose --profile tools run --rm loader
}

# ------------------------------------------------------------
# 2. Validation qualité des référentiels source (Great Expectations)
# ------------------------------------------------------------
Invoke-Step "Validation des référentiels source" {
    docker compose --profile tools run --rm validate-source
}

# ------------------------------------------------------------
# 3. Génération de l'historique d'activités (idempotent : skip si
#    déjà peuplé -- jamais --force ici, une régénération volontaire
#    reste une action manuelle délibérée, hors bootstrap)
# ------------------------------------------------------------
Invoke-Step "Génération de l'historique (12 mois d'activités)" {
    docker compose --profile tools run --rm generateur-historique
}

# ------------------------------------------------------------
# 4. Calcul des distances domicile-travail (idempotent : skip les
#    salariés déjà calculés -- plusieurs minutes seulement au tout
#    premier chargement, quasi instantané ensuite)
# ------------------------------------------------------------
Invoke-Step "Calcul des distances domicile-travail" {
    docker compose --profile tools run --rm compute-distances
}

Write-Host "`n✔ Bootstrap terminé avec succès." -ForegroundColor Green
