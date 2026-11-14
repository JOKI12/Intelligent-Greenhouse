"""
Health check script verifying connectivity across all ecosystem services:
- Mosquitto MQTT
- Apache Kafka
- MinIO S3 Lakehouse
- PostgreSQL Serving Layer
- Grafana UI
"""

import os
import sys
import socket
import psycopg2
import requests

def check_tcp_port(host, port, timeout=3):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False

def check_postgres(host, port, dbname, user, password):
    try:
        conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password, connect_timeout=3)
        conn.close()
        return True, "Connected successfully"
    except Exception as e:
        return False, str(e)

def check_http(url, timeout=3):
    try:
        resp = requests.get(url, timeout=timeout)
        return resp.status_code < 500, f"HTTP {resp.status_code}"
    except Exception as e:
        return False, str(e)

def run_health_check():
    print("==================================================================")
    print("         SERRE AGRICOLE INTELLIGENTE - HEALTH CHECK")
    print("==================================================================")

    services = [
        {"name": "Mosquitto MQTT", "type": "tcp", "host": os.getenv("MQTT_BROKER_HOST", "localhost"), "port": int(os.getenv("MQTT_PORT", 1883))},
        {"name": "Apache Kafka Broker", "type": "tcp", "host": "localhost", "port": int(os.getenv("KAFKA_EXTERNAL_PORT", 29092))},
        {"name": "MinIO S3 API", "type": "http", "url": f"http://localhost:{os.getenv('MINIO_API_PORT', 9000)}/minio/health/live"},
        {"name": "PostgreSQL Serving DB", "type": "pg", "host": os.getenv("POSTGRES_HOST", "localhost"), "port": int(os.getenv("POSTGRES_PORT", 5432)), "db": os.getenv("POSTGRES_DB", "greenhouse_db"), "user": os.getenv("POSTGRES_USER", "postgres"), "pass": os.getenv("POSTGRES_PASSWORD", "postgres")},
        {"name": "Grafana Dashboard", "type": "http", "url": f"http://localhost:{os.getenv('GRAFANA_PORT', 3000)}/api/health"},
        {"name": "Kafka UI", "type": "http", "url": "http://localhost:8080"}
    ]

    all_ok = True
    for s in services:
        status = False
        detail = ""
        if s["type"] == "tcp":
            status = check_tcp_port(s["host"], s["port"])
            detail = f"{s['host']}:{s['port']}"
        elif s["type"] == "http":
            status, detail = check_http(s["url"])
        elif s["type"] == "pg":
            status, detail = check_postgres(s["host"], s["port"], s["db"], s["user"], s["pass"])

        icon = "✅ [OK]" if status else "❌ [DOWN]"
        if not status:
            all_ok = False
        print(f" {icon} {s['name']:<25} : {detail}")

    print("==================================================================")
    if all_ok:
        print(" 🎉 Tous les services sont operationnels et en bonne sante !")
    else:
        print(" ⚠️ Certains services ne repondent pas encore. Verifiez 'docker compose ps'.")
    print("==================================================================")
    return all_ok

if __name__ == "__main__":
    run_health_check()
