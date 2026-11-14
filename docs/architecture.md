# Architecture Système — Serre Agricole Intelligente

## 1. Vue d'Ensemble Globale

La plateforme met en œuvre un pipeline Big Data complet, de la captation IoT distribuée jusqu'à la prise de décision prédictive et la visualisation opérationnelle.

```mermaid
flowchart TD
    subgraph IoT["1. Génération & Collecte IoT"]
        S1["Capteurs Serre 01 (Tomates)"]
        S2["Capteurs Serre 02 (Concombres)"]
        S3["Capteurs Serre 03 (Fraises)"]
        MQTT["Broker MQTT Mosquitto:1883"]
        S1 -->|QoS 1| MQTT
        S2 -->|QoS 1| MQTT
        S3 -->|QoS 1| MQTT
    end

    subgraph Ingestion["2. Passerelle & Streaming"]
        Bridge["Passerelle Python MQTT-to-Kafka"]
        K_MAIN["Kafka Topic: mesures_serre"]
        K_DLQ["Kafka DLQ: mesures_serre_dlq"]
        MQTT --> Bridge
        Bridge -->|Messages Valides| K_MAIN
        Bridge -->|Anomalies & Erreurs| K_DLQ
    end

    subgraph Lakehouse["3. Lakehouse Architecture (MinIO S3 + Hudi)"]
        Bronze["Bronze Layer: Parquet brut append-only"]
        Silver["Silver Layer: Apache Hudi MoR Table"]
        Gold["Gold Layer: Agrégats horaires & journaliers"]
        Bronze --> Silver
        Silver --> Gold
    end

    subgraph Processing["4. Traitement Distribué (Apache Spark)"]
        SSS["Spark Structured Streaming"]
        SQL["Spark SQL Analytics"]
        ML["Spark MLlib (RandomForestRegressor)"]
        K_MAIN --> SSS
        SSS -->|ForeachBatch| Bronze
        SSS -->|Upsert MoR| Silver
        Silver --> SQL
        SQL --> Gold
        Silver --> ML
    end

    subgraph Serving["5. Serving Layer & Décision"]
        PG[("PostgreSQL Serving Database")]
        Weather["Service Météo (OpenWeatherMap)"]
        Airflow["Orchestration Apache Airflow"]
        SSS -->|Temps Réel + Alertes| PG
        SQL -->|Agrégats Horaires/Jour| PG
        ML -->|Inférences Humidité +60min| PG
        Weather -->|Observations Extérieures| PG
        Airflow -.->|Planification| SQL
        Airflow -.->|Ré-entraînement| ML
        Airflow -.->|Collecte| Weather
    end

    subgraph Visualization["6. Visualisation Opérationnelle"]
        Grafana["Tableau de bord Grafana 10"]
        PG --> Grafana
    end
```

---

## 2. Flux Détaillé d'Ingestion & Validation

```mermaid
sequenceDiagram
    participant Sim as Simulateur IoT
    participant MQTT as Mosquitto MQTT
    participant Bridge as Bridge MQTT-Kafka
    participant Kafka as Apache Kafka
    participant Spark as Spark Streaming
    participant PG as PostgreSQL
    participant Lake as MinIO / Hudi

    Sim->>MQTT: Publish serres/{gh}/{sensor}/mesures (JSON)
    MQTT->>Bridge: On message receive
    alt Message conforme aux plages physiques
        Bridge->>Kafka: Send to topic 'mesures_serre'
        Kafka->>Spark: Structured Streaming micro-batch
        Spark->>Lake: Append Bronze Parquet & Upsert Silver Hudi
        Spark->>PG: Upsert latest_measurements
        opt Seuil critique dépassé (Temp > 35°C ou Sol < 25%)
            Spark->>PG: Insert greenhouse_alerts (avec cooldown 3min)
        end
    else Message corrompu ou hors limites
        Bridge->>Kafka: Route vers Dead-Letter Queue (mesures_serre_dlq)
    end
```

---

## 3. Pipeline Machine Learning (Spark MLlib)

```mermaid
flowchart LR
    Hist["Historique Données Silver"] --> Prep["Préparation & Encodage"]
    Prep --> Split["Découpage Chronologique (80/20)"]
    Split --> Train["Entraînement RandomForest"]
    Train --> Eval["Évaluation (RMSE, MAE, R²)"]
    Eval --> Save["Sauvegarde Modèle & Métriques"]
    Save --> Infer["Inférence Humidité Sol (+60min)"]
    Infer --> Decis["Recommandation d'Irrigation"]
    Decis --> PG[("PostgreSQL serving")]
```
