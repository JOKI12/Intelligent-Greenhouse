# ==============================================================================
# Script d'Arret de la Plateforme Serre Agricole Intelligente
# ==============================================================================
Write-Host "Arret des services Docker Compose..." -ForegroundColor Yellow
docker compose down
Write-Host "Services arretes." -ForegroundColor Green
