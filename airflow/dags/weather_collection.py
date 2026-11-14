"""
Airflow DAG: Hourly Weather Collection Service.
Fetches outdoor weather metrics from OpenWeatherMap API.
Provides automatic realistic synthetic fallback if no API key is provided or on rate limit/error.
Persists observations to PostgreSQL 'weather_observations'.
"""

import os
import random
import requests
import psycopg2
from datetime import datetime, timezone, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'greenhouse_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "greenhouse_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres")
    )

def fetch_weather():
    api_key = os.getenv("OPENWEATHER_API_KEY", "").strip()
    city = os.getenv("OPENWEATHER_CITY", "Paris,FR")
    now_ts = datetime.now(timezone.utc)

    weather_data = None
    is_synthetic = False

    if api_key:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                weather_data = {
                    "city": city,
                    "temp": float(data["main"]["temp"]),
                    "humidity": float(data["main"]["humidity"]),
                    "wind_speed": float(data.get("wind", {}).get("speed", 0.0)),
                    "rainfall": float(data.get("rain", {}).get("1h", 0.0)),
                    "description": data["weather"][0]["description"]
                }
                print(f"[WEATHER] Successfully fetched live weather for {city}: {weather_data}")
            else:
                print(f"[WEATHER] OpenWeather API returned status {resp.status_code}. Using synthetic fallback.")
        except Exception as e:
            print(f"[WEATHER] Error querying OpenWeather API: {e}. Using synthetic fallback.")

    if not weather_data:
        is_synthetic = True
        hour = now_ts.hour
        base_temp = 15.0 + 8.0 * (1.0 if 11 <= hour <= 16 else 0.2)
        weather_data = {
            "city": city,
            "temp": round(base_temp + random.uniform(-1.5, 1.5), 1),
            "humidity": round(random.uniform(45.0, 80.0), 1),
            "wind_speed": round(random.uniform(2.0, 15.0), 1),
            "rainfall": round(random.choice([0.0, 0.0, 0.0, 1.2, 3.5]), 1),
            "description": random.choice(["clear sky", "few clouds", "scattered clouds", "light rain"])
        }
        print(f"[WEATHER-SYNTHETIC] Generated realistic weather observation: {weather_data}")

    # Persist to PostgreSQL
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO weather_observations
        (timestamp, city, outdoor_temperature, outdoor_humidity, wind_speed, rainfall_1h, weather_description, is_synthetic)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        now_ts, weather_data["city"], weather_data["temp"], weather_data["humidity"],
        weather_data["wind_speed"], weather_data["rainfall"], weather_data["description"], is_synthetic
    ))
    conn.commit()
    cur.close()
    conn.close()
    print("[WEATHER] Successfully saved weather observation to PostgreSQL.")

with DAG(
    'hourly_weather_collection',
    default_args=default_args,
    description='Collect external outdoor weather conditions with synthetic fallback',
    schedule_interval='@hourly',
    catchup=False,
    tags=['greenhouse', 'weather']
) as dag:

    collect_weather_task = PythonOperator(
        task_id='fetch_and_store_weather',
        python_callable=fetch_weather
    )
