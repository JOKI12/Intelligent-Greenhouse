"""
Script de generation de donnees de demonstration initiales.
Insere des mesures realistes, alertes, observations meteo et predictions dans PostgreSQL
pour permettre une demonstration immediate et l'affichage complet du dashboard Grafana.
"""

import os
import random
import uuid
import psycopg2
from datetime import datetime, timezone, timedelta

def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "greenhouse_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres")
    )

def generate_demo_data():
    print("[DEMO-DATA] Generation du jeu de donnees de demonstration initial...")
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
    except Exception as e:
        print(f"[DEMO-DATA] PostgreSQL non joignable ({e}). Veuillez demarrer le conteneur postgres.")
        return False

    now = datetime.now(timezone.utc)
    greenhouses = ["serre_01", "serre_02", "serre_03"]
    sensors = ["capteur_01", "capteur_02"]

    # 1. Latest measurements
    for gh in greenhouses:
        for s in sensors:
            temp = 22.0 + random.uniform(1.0, 8.0)
            hum = 60.0 + random.uniform(-10.0, 15.0)
            soil = 28.0 + random.uniform(-8.0, 30.0)
            lux = random.uniform(5000.0, 35000.0)
            co2 = random.uniform(450.0, 750.0)
            status = "OK"

            cur.execute("""
                INSERT INTO latest_measurements 
                (greenhouse_id, sensor_id, temperature, humidity, soil_moisture, light_lux, co2, status, timestamp, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (greenhouse_id, sensor_id) DO UPDATE SET
                    temperature = EXCLUDED.temperature,
                    humidity = EXCLUDED.humidity,
                    soil_moisture = EXCLUDED.soil_moisture,
                    light_lux = EXCLUDED.light_lux,
                    co2 = EXCLUDED.co2,
                    status = EXCLUDED.status,
                    timestamp = EXCLUDED.timestamp,
                    updated_at = CURRENT_TIMESTAMP;
            """, (gh, s, round(temp, 2), round(hum, 2), round(soil, 2), round(lux, 1), round(co2, 1), status, now))

    # 2. Hourly metrics (last 12 hours)
    for h in range(12, 0, -1):
        w_start = now - timedelta(hours=h)
        w_end = w_start + timedelta(hours=1)
        for gh in greenhouses:
            cur.execute("""
                INSERT INTO greenhouse_hourly_metrics
                (greenhouse_id, window_start, window_end, avg_temperature, min_temperature, max_temperature,
                 avg_humidity, avg_soil_moisture, min_soil_moisture, max_soil_moisture, avg_light_lux, avg_co2, measurement_count, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (greenhouse_id, window_start) DO NOTHING;
            """, (
                gh, w_start, w_end,
                round(24.0 + random.uniform(-2, 4), 2),
                round(20.0 + random.uniform(-1, 2), 2),
                round(28.0 + random.uniform(0, 3), 2),
                round(65.0 + random.uniform(-5, 10), 2),
                round(45.0 + random.uniform(-10, 15), 2),
                round(35.0, 2), round(55.0, 2),
                round(random.uniform(10000, 30000), 1),
                round(random.uniform(500, 700), 1),
                120
            ))

    # 3. Alerts
    alerts = [
        ("serre_01", "capteur_02", "soil_moisture", 21.4, 22.0, "CRITICAL", "Humidité du sol critique (< 22%) : Risque de flétrissement des plants de tomates"),
        ("serre_02", "capteur_01", "temperature", 36.8, 35.0, "CRITICAL", "Température excessive (> 35°C) : Aération d'urgence recommandée"),
        ("serre_03", "capteur_01", "humidity", 28.5, 30.0, "WARNING", "Baisse de l'humidité relative de l'air sous le seuil de confort")
    ]
    for gh, s, param, val, thresh, sev, desc in alerts:
        a_id = f"ALT_{uuid.uuid4().hex[:10].upper()}"
        cur.execute("""
            INSERT INTO greenhouse_alerts
            (alert_id, greenhouse_id, sensor_id, parameter, value, threshold, severity, description, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (alert_id) DO NOTHING;
        """, (a_id, gh, s, param, val, thresh, sev, desc, now - timedelta(minutes=random.randint(5, 120))))

    # 4. Weather Observations
    cur.execute("""
        INSERT INTO weather_observations
        (timestamp, city, outdoor_temperature, outdoor_humidity, wind_speed, rainfall_1h, weather_description, is_synthetic)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """, (now, "Paris,FR", 19.5, 62.0, 8.4, 0.0, "Ciel partiellement voilé", True))

    # 5. Irrigation Predictions
    predictions = [
        ("serre_01", 24.5, 21.8, True, "Humidité prédite sous le seuil critique (21.8% <= 30.0%). Irrigation planifiée à T+45min."),
        ("serre_02", 42.0, 39.5, False, "Conditions d'humidité hydrique optimales. Aucune irrigation requise."),
        ("serre_03", 26.0, 23.2, True, "Baisse accélérée de l'humidité du substrat. Déclenchement recommandé de la micro-irrigation.")
    ]
    for gh, cur_m, pred_m, rec, msg in predictions:
        cur.execute("""
            INSERT INTO irrigation_predictions
            (greenhouse_id, timestamp, current_soil_moisture, predicted_moisture_60min, irrigation_recommended, recommendation_message, model_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (gh, now, cur_m, pred_m, rec, msg, "v1.0.0"))

    # 6. ML Model Metrics
    cur.execute("""
        INSERT INTO model_evaluation_metrics
        (model_name, version, training_timestamp, rmse, mae, r2, samples_count, features_used)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """, ("RandomForestRegressor", "v1.0.0", now - timedelta(days=1), 0.8421, 0.6134, 0.9412, 3500, "greenhouse_id, temperature, humidity, soil_moisture, light_lux, co2"))

    conn.commit()
    cur.close()
    conn.close()
    print("[DEMO-DATA] Donnees de demonstration injectees avec succes !")
    return True

if __name__ == "__main__":
    generate_demo_data()
