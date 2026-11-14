# ==============================================================================
# Script de Demarrage de la Plateforme Serre Agricole Intelligente (PowerShell)
# ==============================================================================

Write-Host "==================================================================" -ForegroundColor Green
Write-Host "  SERRE AGRICOLE INTELLIGENTE - PLATEFORME BIG DATA & IOT" -ForegroundColor Green
Write-Host "==================================================================" -ForegroundColor Green

if (-not (Test-Path .env)) {
    Write-Host "[!] Fichier .env manquant. Copie depuis .env.example..." -ForegroundColor Yellow
    Copy-Item .env.example .env
}

Write-Host "[1/4] Demarrage de l'infrastructure Docker..." -ForegroundColor Cyan
docker compose up -d

Write-Host "[2/4] Attente de la disponibilite des services..." -ForegroundColor Cyan
Start-Sleep -Seconds 10

Write-Host "[3/4] Verification de l'etat des conteneurs..." -ForegroundColor Cyan
docker compose ps

Write-Host ""
Write-Host "[4/4] Pret ! Interfaces disponibles :" -ForegroundColor Green
Write-Host " - Grafana Monitoring : http://localhost:3000 (admin / admin)"
Write-Host " - Kafka UI Management: http://localhost:8080"
Write-Host " - Apache Airflow UI  : http://localhost:8085 (admin / admin)"
Write-Host " - MinIO S3 Console   : http://localhost:9001 (minioadmin / minioadmin)"
Write-Host ""
Write-Host "Pour lancer le simulateur IoT en continu :" -ForegroundColor Yellow
Write-Host "  python simulator/sensor_simulator.py"
Write-Host "==================================================================" -ForegroundColor Green
