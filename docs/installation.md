# Guide d'Installation et de Déploiement

## 1. Prérequis Système

| Ressource | Recommandation Minimale | Recommandation Optimale |
|---|---|---|
| **OS** | Windows 10/11 avec WSL2 ou Linux / macOS | Linux / Ubuntu 22.04 LTS ou WSL2 |
| **CPU** | 4 cœurs | 8 cœurs |
| **RAM** | 8 Go | 16 Go |
| **Disque** | 15 Go d'espace libre | 30 Go SSD |
| **Docker** | Docker Desktop 4.x+ avec Docker Compose v2+ | Docker Engine & Compose v2.20+ |
| **Python** | Python 3.9+ | Python 3.9 - 3.11 |

---

## 2. Démarrage Rapide en 3 Étapes

### Étape 1 : Configuration de l'environnement
Copier le fichier de variables d'environnement type :
```bash
cp .env.example .env
```
*(Optionnel) Si vous disposez d'une clé OpenWeatherMap, renseignez-la dans `.env` sous `OPENWEATHER_API_KEY`. En l'absence de clé, la plateforme bascule automatiquement en mode synthétique réaliste sans interruption.*

### Étape 2 : Lancement des conteneurs
Sous Linux / macOS / WSL :
```bash
bash scripts/start.sh
```
Sous Windows PowerShell :
```powershell
.\scripts\start.ps1
```

### Étape 3 : Vérification de la santé des services
```bash
python scripts/health_check.py
```

---

## 3. Interfaces Web Disponibles

Une fois l'infrastructure démarrée :

| Interface | URL d'Accès | Identifiants par défaut | Description |
|---|---|---|---|
| **Grafana** | `http://localhost:3000` | `admin` / `admin` | Tableau de bord de supervision des serres |
| **Kafka UI** | `http://localhost:8080` | Libre d'accès | Inspection des topics, partitions et messages |
| **Apache Airflow** | `http://localhost:8085` | `admin` / `admin` | Gestion des DAGs et tâches d'automatisation |
| **MinIO Console** | `http://localhost:9001` | `minioadmin` / `minioadmin` | Exploration du Lakehouse S3 (Bronze, Silver, Gold) |

---

## 4. Démarrage des Composants Applicatifs

### A. Lancer la Passerelle MQTT-vers-Kafka
Dans un terminal dédié :
```bash
python kafka/mqtt_kafka_bridge.py
```

### B. Lancer le Simulateur de Capteurs IoT
Dans un second terminal :
```bash
python simulator/sensor_simulator.py --interval 3
```

### C. Lancer le Job Spark Streaming
```bash
python spark/jobs/streaming_ingestion.py
```

### D. Injecter des Données de Démonstration Immédiates
Pour visualiser immédiatement le tableau de bord avec des graphiques complets :
```bash
python scripts/generate_demo_data.py
```
