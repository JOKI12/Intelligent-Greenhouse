# ==============================================================================
# Script de Demarrage de la Plateforme Serre Agricole Intelligente (Bash/Linux/WSL)
# ==============================================================================
set -e

echo "=================================================================="
echo "  SERRE AGRICOLE INTELLIGENTE - PLATEFORME BIG DATA & IOT"
echo "=================================================================="

if [ ! -f .env ]; then
    echo "[!] Fichier .env manquant. Copie depuis .env.example..."
    cp .env.example .env
fi

echo "[1/4] Demarrage de l'infrastructure Docker..."
docker compose up -d

echo "[2/4] Attente de la disponibilite des services..."
sleep 10

echo "[3/4] Verification de l'etat des conteneurs..."
docker compose ps

echo "[4/4] Pret ! Interfaces disponibles :"
echo " - Grafana Monitoring : http://localhost:3000 (admin / admin)"
echo " - Kafka UI Management: http://localhost:8080"
echo " - Apache Airflow UI  : http://localhost:8085 (admin / admin)"
echo " - MinIO S3 Console   : http://localhost:9001 (minioadmin / minioadmin)"
echo ""
echo "Pour lancer le simulateur IoT en continu :"
echo "  python simulator/sensor_simulator.py"
echo "=================================================================="
