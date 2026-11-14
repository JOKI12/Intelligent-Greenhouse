# Guide de Démonstration et Scénarios d'Évaluation

Ce guide détaille le scénario complet de validation et de soutenance académique de la plateforme **Serre Agricole Intelligente**.

---

## Scénario 1 : Génération et Acheminement des Flux IoT (MQTT & Kafka)

1. Démarrer le simulateur :
   ```bash
   python simulator/sensor_simulator.py --interval 2
   ```
2. Observer l'émission sur les 3 serres (`serre_01`, `serre_02`, `serre_03`) avec prise en compte du cycle diurne (soleil, température, évaporation progressive de l'humidité du sol).
3. Ouvrir l'interface **Kafka UI** sur `http://localhost:8080` :
   - Naviguer vers le topic `mesures_serre`.
   - Observer l'arrivée continue des messages JSON enrichis d'un `ingestion_timestamp`.
   - Inspecter le topic Dead-Letter Queue `mesures_serre_dlq` qui capture toute trame corrompue ou valeur aberrante.

---

## Scénario 2 : Détection d'Anomalies et Gestion des Alertes

1. Le simulateur injecte occasionnellement (probabilité de 5%) une anomalie thermique (canicule > 35°C) ou hydrique (sol desséché < 22%).
2. Spark Structured Streaming intercepte la condition hors seuil :
   - Déclenchement d'une alerte avec sévérité `CRITICAL` ou `WARNING`.
   - Application d'un mécanisme de temporisation (*cooldown* de 3 minutes) pour éviter le spam d'alertes identiques.
3. Vérification :
   - L'alerte apparaît en temps réel dans PostgreSQL (`greenhouse_alerts`).
   - Le panneau **🚨 CENTRE D'ALERTES** de Grafana met à jour le compteur et la table d'incidents.

---

## Scénario 3 : Entraînement et Inférence Prédictive (Spark MLlib)

1. Exécuter l'entraînement du modèle Random Forest :
   ```bash
   python spark/jobs/train_irrigation_model.py
   ```
   - Validation du split chronologique.
   - Affichage des métriques : $RMSE \approx 0.84$, $MAE \approx 0.61$, $R^2 \approx 0.94$.
   - Enregistrement du modèle sérialisé sous `spark/models/rf_irrigation_model`.
2. Exécuter le job d'aide à la décision :
   ```bash
   python spark/jobs/irrigation_prediction.py
   ```
   - Prédiction de l'humidité du sol à +60 minutes.
   - Si $Humidit\acute{e}_{predite} \le Seuil_{serre}$, émission d'une recommandation : `🚨 OUI (Irrigation Requise)`.
3. Constater l'affichage immédiat dans Grafana sous la section **🤖 MACHINE LEARNING & IRRIGATION PRÉDICTIVE**.

---

## Scénario 4 : Orchestration des Workflows (Apache Airflow)

1. Accéder à l'interface Airflow sur `http://localhost:8085` (identifiants `admin`/`admin`).
2. Déclencher manuellement le DAG `hourly_weather_collection` :
   - Tâche de collecte météo exécutée avec succès (OpenWeatherMap ou fallback synthétique).
3. Déclencher le DAG `daily_greenhouse_analytics` :
   - Calcul des agrégats journaliers.
   - Génération et export d'un rapport de synthèse sous `/tmp/greenhouse_reports/`.
4. Déclencher le DAG `ml_irrigation_training` pour attester du ré-entraînement automatisé.

---

## Scénario 5 : Surveillance Globale sur Grafana

1. Ouvrir `http://localhost:3000` (identifiants `admin`/`admin`).
2. Accéder au tableau de bord provisionné automatiquement : **SMART GREENHOUSE — Agricultural Intelligence Platform**.
3. Présenter les 5 sections ergonomiques :
   - **Vue Globale** : Cartouches indicateurs (Serres actives, Capteurs, Alertes, Serres à irriguer).
   - **Télémetrie Temps-Réel** : Courbes dynamiques de température, humidité de l'air et humidité du sol.
   - **Machine Learning** : Tableau comparatif humidité actuelle vs prédiction à 1 heure et recommandation.
   - **Journal des Alertes** : Historique des anomalies détectées.
   - **Météo** : Température extérieure et précipitations.
