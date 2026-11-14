# Dictionnaire des Données — Plateforme Serre Agricole Intelligente

Ce document répertorie l'ensemble des grandeurs physiques mesurées, leurs unités, plages admissibles, formats d'échange et schémas relationnels / Lakehouse.

---

## 1. Télémetrie Capteurs IoT (Topic MQTT & Message Kafka)

Format : `JSON`  
Encodage : `UTF-8`

| Champ | Type | Description | Unité | Plage Normale | Plage Alerte |
|---|---|---|---|---|---|
| `greenhouse_id` | String | Identifiant unique de la serre (`serre_01`, `serre_02`, `serre_03`) | - | - | - |
| `sensor_id` | String | Identifiant unique du capteur (`capteur_01`, `capteur_02`) | - | - | - |
| `crop` | String | Type de culture sous serre (`Tomatoes`, `Cucumbers`, `Strawberries`) | - | - | - |
| `temperature` | Float | Température ambiante mesurée sous abri | °C | 18.0 - 28.0 | < 10.0 ou > 35.0 |
| `humidity` | Float | Humidité relative de l'air | % | 50.0 - 80.0 | < 30.0 ou > 90.0 |
| `soil_moisture` | Float | Teneur volumique en eau du substrat (sol) | % | 40.0 - 75.0 | < 25.0 (Irrigation critique) |
| `light_lux` | Float | Éclairement lumineux reçu au niveau du feuillage | lux | 0.0 - 50 000.0 | - |
| `co2` | Float | Concentration en dioxyde de carbone | ppm | 400.0 - 800.0 | > 1200.0 |
| `status` | String | État d'intégrité de la sonde (`OK`, `WARNING`, `ERROR`) | - | - | - |
| `timestamp` | String (ISO 8601)| Horodatage UTC d'émission par le capteur | - | - | - |
| `ingestion_timestamp` | String (ISO 8601)| Horodatage d'arrivée sur la passerelle Kafka | - | - | - |

---

## 2. Lakehouse Architecture (MinIO / Apache Hudi)

### A. Bronze Layer (`s3a://lakehouse/bronze/mesures_serre/`)
- Format : Parquet append-only brut.
- Partitionnement : `greenhouse_id`.
- Rôle : Traçabilité immuable, conservation intégrale des flux IoT sans filtrage destructif.

### B. Silver Layer (`silver_mesures_serre`)
- Format : Table Apache Hudi MoR (Merge-on-Read).
- Clé d'enregistrement (`record_key`) : `id` (`greenhouse_id` + `sensor_id` + `timestamp`).
- Partitionnement : `greenhouse_id`.
- Precombine field : `timestamp`.
- Rôle : Déduplication, nettoyage, validation des plages physiques et jointure des dimensions.

### C. Gold Layer (`s3a://lakehouse/gold/greenhouse_hourly_metrics`)
- Format : Parquet / Hudi.
- Granularité : Agrégation horaire par serre.
- Métriques : Moyennes, minima, maxima, compteurs d'alertes.

---

## 3. Schéma Serving PostgreSQL (`greenhouse_db`)

- `latest_measurements` : État instantané en temps réel de chaque capteur pour affichage Grafana sans latence.
- `greenhouse_hourly_metrics` : Métriques agrégées par heure pour graphiques historiques.
- `greenhouse_daily_metrics` : Synthèse quotidienne pour rapports et analyses de long terme.
- `greenhouse_alerts` : Journal horodaté des alertes avec niveau de sévérité (`CRITICAL`, `WARNING`).
- `weather_observations` : Conditions météorologiques extérieures (OpenWeatherMap / Synthétique).
- `irrigation_predictions` : Résultats d'inférence Spark MLlib et recommandations d'arrosage.
- `model_evaluation_metrics` : Traçabilité des versions d'algorithmes et métriques (RMSE, MAE, R²).
