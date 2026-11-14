"""
Airflow DAG: Daily Greenhouse Analytics & Reporting.
Reads historical Silver data, calculates daily metrics, updates Gold datasets,
and exports a daily operational summary report (JSON/CSV).
"""

import os
import json
import csv
import psycopg2
from datetime import datetime, timezone, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'greenhouse_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "greenhouse_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres")
    )

def run_analytics_task():
    print("[AIRFLOW-ANALYTICS] Triggering greenhouse analytics computation...")
    # Calls Spark Analytics job or runs SQL rollup
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO greenhouse_daily_metrics (greenhouse_id, metric_date, avg_temperature, min_temperature, max_temperature, avg_humidity, avg_soil_moisture, total_alerts, updated_at)
        SELECT 
            m.greenhouse_id,
            DATE(m.timestamp) as metric_date,
            ROUND(AVG(m.temperature)::numeric, 2),
            ROUND(MIN(m.temperature)::numeric, 2),
            ROUND(MAX(m.temperature)::numeric, 2),
            ROUND(AVG(m.humidity)::numeric, 2),
            ROUND(AVG(m.soil_moisture)::numeric, 2),
            COALESCE((SELECT COUNT(*) FROM greenhouse_alerts a WHERE a.greenhouse_id = m.greenhouse_id AND DATE(a.timestamp) = DATE(m.timestamp)), 0)::int as total_alerts,
            CURRENT_TIMESTAMP
        FROM latest_measurements m
        GROUP BY m.greenhouse_id, DATE(m.timestamp)
        ON CONFLICT (greenhouse_id, metric_date) DO UPDATE SET
            avg_temperature = EXCLUDED.avg_temperature,
            min_temperature = EXCLUDED.min_temperature,
            max_temperature = EXCLUDED.max_temperature,
            avg_humidity = EXCLUDED.avg_humidity,
            avg_soil_moisture = EXCLUDED.avg_soil_moisture,
            total_alerts = EXCLUDED.total_alerts,
            updated_at = CURRENT_TIMESTAMP;
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("[AIRFLOW-ANALYTICS] Successfully updated daily metrics.")

def export_daily_report():
    print("[AIRFLOW-REPORT] Generating daily greenhouse summary report...")
    out_dir = "/tmp/greenhouse_reports"
    os.makedirs(out_dir, exist_ok=True)
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    report_file = os.path.join(out_dir, f"greenhouse_report_{today_str}.json")

    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT greenhouse_id, metric_date, avg_temperature, min_temperature, max_temperature, avg_humidity, avg_soil_moisture, total_alerts
        FROM greenhouse_daily_metrics
        ORDER BY metric_date DESC, greenhouse_id ASC;
    """)
    rows = cur.fetchall()
    report = []
    for r in rows:
        report.append({
            "greenhouse_id": r[0],
            "date": str(r[1]),
            "avg_temp": float(r[2]) if r[2] is not None else None,
            "min_temp": float(r[3]) if r[3] is not None else None,
            "max_temp": float(r[4]) if r[4] is not None else None,
            "avg_humidity": float(r[5]) if r[5] is not None else None,
            "avg_soil_moisture": float(r[6]) if r[6] is not None else None,
            "total_alerts": r[7]
        })
    cur.close()
    conn.close()

    with open(report_file, "w") as f:
        json.dump(report, f, indent=2)

    print(f"[AIRFLOW-REPORT] Daily report exported to: {report_file}")

with DAG(
    'daily_greenhouse_analytics',
    default_args=default_args,
    description='Compute daily greenhouse aggregates and generate summary report',
    schedule_interval='@daily',
    catchup=False,
    tags=['greenhouse', 'analytics']
) as dag:

    calculate_metrics = PythonOperator(
        task_id='calculate_daily_metrics',
        python_callable=run_analytics_task
    )

    generate_report = PythonOperator(
        task_id='export_daily_report',
        python_callable=export_daily_report
    )

    calculate_metrics >> generate_report
