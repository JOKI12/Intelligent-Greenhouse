"""
Spark Structured Streaming Job:
1. Consumes telemetry from Kafka topic 'mesures_serre'.
2. Parses & enforces schema with Event-Time & Watermark.
3. Persists raw events to Lakehouse Bronze layer (s3a://lakehouse/bronze/mesures_serre/).
4. Cleanses, deduplicates, and writes to Lakehouse Silver Hudi MoR table (s3a://lakehouse/silver/silver_mesures_serre).
5. Detects real-time threshold breaches (Alerts) with cooldown and persists to PostgreSQL.
6. Updates 'latest_measurements' in PostgreSQL Serving Layer.
"""

import os
import sys
import uuid
import psycopg2
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp, date_format, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

try:
    from spark.config.spark_config import get_spark_session
    from spark.config.alert_rules import ALERT_THRESHOLDS
except ImportError:
    from config.spark_config import get_spark_session
    from config.alert_rules import ALERT_THRESHOLDS

# In-memory cooldown cache for alerts: (greenhouse_id, parameter) -> last_alert_timestamp
alert_cooldown_cache = {}

def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "greenhouse_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres")
    )

def process_batch(df, batch_id):
    """
    ForeachBatch micro-batch processor:
    - Writes to Bronze Parquet (append-only)
    - Writes/Upserts to Silver Hudi MoR Table
    - Updates PostgreSQL latest_measurements
    - Evaluates Alerts with cooldown and inserts into PostgreSQL greenhouse_alerts
    """
    if df.rdd.isEmpty():
        return

    print(f"\n[STREAMING] Processing micro-batch {batch_id} with {df.count()} records...")

    # 1. Bronze Persistence (MinIO S3A)
    bronze_path = "s3a://lakehouse/bronze/mesures_serre"
    try:
        df.write \
          .mode("append") \
          .partitionBy("greenhouse_id") \
          .parquet(bronze_path)
        print(f"[STREAMING] Successfully appended micro-batch {batch_id} to Bronze ({bronze_path})")
    except Exception as e:
        print(f"[STREAMING] Warning writing to Bronze S3A: {e}. Writing to local fallback bronze.")
        try:
            df.write.mode("append").partitionBy("greenhouse_id").parquet("./lakehouse/bronze/mesures_serre")
        except Exception as ex2:
            print(f"[STREAMING] Local Bronze fallback error: {ex2}")

    # 2. Silver Hudi MoR Upsert
    silver_path = "s3a://lakehouse/silver/silver_mesures_serre"
    hudi_options = {
        'hoodie.table.name': 'silver_mesures_serre',
        'hoodie.datasource.write.table.type': 'MERGE_ON_READ',
        'hoodie.datasource.write.recordkey.field': 'record_key',
        'hoodie.datasource.write.partitionpath.field': 'greenhouse_id',
        'hoodie.datasource.write.precombine.field': 'timestamp',
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.SimpleKeyGenerator'
    }

    # Add stable record_key: greenhouse_id + '_' + sensor_id + '_' + timestamp
    silver_df = df.withColumn("record_key", col("id"))

    try:
        silver_df.write.format("hudi") \
                 .options(**hudi_options) \
                 .mode("append") \
                 .save(silver_path)
        print(f"[STREAMING] Successfully upserted micro-batch {batch_id} into Silver Hudi MoR ({silver_path})")
    except Exception as e:
        print(f"[STREAMING] Hudi S3A Upsert note: {e}. Executing Silver parquet sync fallback.")
        try:
            silver_df.write.mode("append").partitionBy("greenhouse_id").parquet("./lakehouse/silver/silver_mesures_serre")
        except Exception as ex2:
            print(f"[STREAMING] Silver local fallback note: {ex2}")

    # 3. Serving Layer Sync (PostgreSQL) & Alert Detection
    rows = df.collect()
    try:
        conn = get_pg_connection()
        cur = conn.cursor()

        for r in rows:
            gh_id = r["greenhouse_id"]
            sensor_id = r["sensor_id"]
            temp = r["temperature"]
            hum = r["humidity"]
            soil = r["soil_moisture"]
            lux = r["light_lux"]
            co2 = r["co2"]
            status = r["status"]
            ts = r["event_timestamp"]

            # Upsert into latest_measurements
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
            """, (gh_id, sensor_id, temp, hum, soil, lux, co2, status, ts))

            # Threshold Evaluation & Alert Generation with Cooldown
            now_epoch = datetime.now(timezone.utc).timestamp()
            alerts_to_check = []

            # Check Temperature
            if temp is not None:
                if temp > ALERT_THRESHOLDS["temperature"]["max"]:
                    alerts_to_check.append(("temperature", temp, ALERT_THRESHOLDS["temperature"]["max"], "CRITICAL", f"High Temperature Warning: {temp:.1f}°C"))
                elif temp < ALERT_THRESHOLDS["temperature"]["min"]:
                    alerts_to_check.append(("temperature", temp, ALERT_THRESHOLDS["temperature"]["min"], "WARNING", f"Low Temperature Alert: {temp:.1f}°C"))

            # Check Humidity
            if hum is not None:
                if hum < ALERT_THRESHOLDS["humidity"]["min"]:
                    alerts_to_check.append(("humidity", hum, ALERT_THRESHOLDS["humidity"]["min"], "CRITICAL", f"Critical Low Humidity: {hum:.1f}%"))
                elif hum > ALERT_THRESHOLDS["humidity"]["max"]:
                    alerts_to_check.append(("humidity", hum, ALERT_THRESHOLDS["humidity"]["max"], "WARNING", f"High Humidity: {hum:.1f}%"))

            # Check Soil Moisture
            if soil is not None:
                if soil < ALERT_THRESHOLDS["soil_moisture"]["min_critical"]:
                    alerts_to_check.append(("soil_moisture", soil, ALERT_THRESHOLDS["soil_moisture"]["min_critical"], "CRITICAL", f"Soil Moisture Critically Low: {soil:.1f}%. Immediate irrigation needed!"))
                elif soil < ALERT_THRESHOLDS["soil_moisture"]["min_warning"]:
                    alerts_to_check.append(("soil_moisture", soil, ALERT_THRESHOLDS["soil_moisture"]["min_warning"], "WARNING", f"Soil Moisture Dropping: {soil:.1f}%."))

            for param, val, thresh, sev, desc in alerts_to_check:
                cache_key = (gh_id, param)
                last_alert_time = alert_cooldown_cache.get(cache_key, 0)
                # 3 minutes cooldown
                if (now_epoch - last_alert_time) > 180:
                    alert_cooldown_cache[cache_key] = now_epoch
                    alert_id = f"ALT_{uuid.uuid4().hex[:12].upper()}"
                    cur.execute("""
                        INSERT INTO greenhouse_alerts
                        (alert_id, greenhouse_id, sensor_id, parameter, value, threshold, severity, description, timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (alert_id, gh_id, sensor_id, param, val, thresh, sev, desc, ts))
                    print(f"[ALERT TRIGGERED] [{sev}] {desc} for {gh_id}/{sensor_id}")

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[STREAMING] Error updating PostgreSQL Serving layer: {e}")


def main():
    print("[STREAMING] Initializing Spark Structured Streaming Application...")
    spark = get_spark_session(app_name="GreenhouseStreamingIngestion", is_streaming=True)
    spark.sparkContext.setLogLevel("WARN")

    kafka_bootstrap = os.getenv("KAFKA_BROKER", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC_MEASURES", "mesures_serre")

    schema = StructType([
        StructField("greenhouse_id", StringType(), False),
        StructField("sensor_id", StringType(), False),
        StructField("crop", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("soil_moisture", DoubleType(), True),
        StructField("light_lux", DoubleType(), True),
        StructField("co2", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("timestamp", StringType(), False),
        StructField("ingestion_timestamp", StringType(), True)
    ])

    print(f"[STREAMING] Subscribing to Kafka: {kafka_bootstrap}, Topic: {kafka_topic}")

    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed_df = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*") \
        .filter(col("greenhouse_id").isNotNull() & col("sensor_id").isNotNull()) \
        .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
        .withWatermark("event_timestamp", "5 minutes") \
        .withColumn("id", col("greenhouse_id") + lit("_") + col("sensor_id") + lit("_") + col("timestamp"))

    checkpoint_dir = "/tmp/spark_checkpoints/greenhouse_stream"

    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", checkpoint_dir) \
        .start()

    print("[STREAMING] Streaming query started. Awaiting termination...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
