"""
Spark Batch Analytics Job:
1. Reads historical measurements from Silver Hudi / Lakehouse layer.
2. Computes hourly aggregates (avg/min/max temperature, humidity, soil moisture, light, count).
3. Computes daily greenhouse metrics.
4. Writes aggregates to Lakehouse Gold Layer (s3a://lakehouse/gold/greenhouse_hourly_metrics).
5. Persists dashboard-ready aggregates into PostgreSQL serving tables:
   - greenhouse_hourly_metrics
   - greenhouse_daily_metrics
"""

import os
import sys
import psycopg2
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, window, avg, min, max, count, date_trunc, to_date, round as spark_round
)

try:
    from spark.config.spark_config import get_spark_session
except ImportError:
    from config.spark_config import get_spark_session

def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "greenhouse_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres")
    )

def run_analytics():
    print("[ANALYTICS] Starting Spark Greenhouse Analytics computation...")
    spark = get_spark_session(app_name="GreenhouseAnalytics")
    spark.sparkContext.setLogLevel("WARN")

    silver_s3_path = "s3a://lakehouse/silver/silver_mesures_serre"
    silver_local_path = "./lakehouse/silver/silver_mesures_serre"

    # Try reading from S3A Hudi / Parquet, fallback to local
    df = None
    try:
        print(f"[ANALYTICS] Attempting to read Silver layer from {silver_s3_path}...")
        df = spark.read.format("hudi").load(silver_s3_path)
    except Exception as e:
        print(f"[ANALYTICS] Could not read as Hudi S3: {e}. Trying S3A parquet...")
        try:
            df = spark.read.parquet(silver_s3_path)
        except Exception as e2:
            print(f"[ANALYTICS] S3A read failed: {e2}. Reading from local silver directory...")
            if os.path.exists(silver_local_path):
                df = spark.read.parquet(silver_local_path)

    # Fallback to querying PostgreSQL if Silver Lakehouse is not yet populated
    if df is None or df.count() == 0:
        print("[ANALYTICS] Silver layer empty. Reading latest historical measurements from PostgreSQL serving table...")
        pg_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', 5432)}/{os.getenv('POSTGRES_DB', 'greenhouse_db')}"
        df = spark.read \
            .format("jdbc") \
            .option("url", pg_url) \
            .option("dbtable", "latest_measurements") \
            .option("user", os.getenv("POSTGRES_USER", "postgres")) \
            .option("password", os.getenv("POSTGRES_PASSWORD", "postgres")) \
            .option("driver", "org.postgresql.Driver") \
            .load()

    if df.count() == 0:
        print("[ANALYTICS] No records found to analyze. Exiting.")
        return

    print(f"[ANALYTICS] Successfully loaded {df.count()} records for analytics.")

    # Normalize timestamp column
    if "timestamp" in df.columns:
        df_clean = df.withColumn("event_time", to_timestamp(col("timestamp")))
    elif "event_timestamp" in df.columns:
        df_clean = df.withColumn("event_time", col("event_timestamp"))
    else:
        df_clean = df.withColumn("event_time", col("updated_at"))

    # 1. Hourly Aggregations
    hourly_df = df_clean.groupBy(
        col("greenhouse_id"),
        window(col("event_time"), "1 hour")
    ).agg(
        spark_round(avg("temperature"), 2).alias("avg_temperature"),
        spark_round(min("temperature"), 2).alias("min_temperature"),
        spark_round(max("temperature"), 2).alias("max_temperature"),
        spark_round(avg("humidity"), 2).alias("avg_humidity"),
        spark_round(avg("soil_moisture"), 2).alias("avg_soil_moisture"),
        spark_round(min("soil_moisture"), 2).alias("min_soil_moisture"),
        spark_round(max("soil_moisture"), 2).alias("max_soil_moisture"),
        spark_round(avg("light_lux"), 1).alias("avg_light_lux"),
        spark_round(avg("co2"), 1).alias("avg_co2"),
        count("*").alias("measurement_count")
    ).select(
        col("greenhouse_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_temperature"),
        col("min_temperature"),
        col("max_temperature"),
        col("avg_humidity"),
        col("avg_soil_moisture"),
        col("min_soil_moisture"),
        col("max_soil_moisture"),
        col("avg_light_lux"),
        col("avg_co2"),
        col("measurement_count")
    )

    print("[ANALYTICS] Hourly metrics calculated:")
    hourly_df.show(5, truncate=False)

    # Write to Gold Layer (Lakehouse)
    gold_hourly_path = "s3a://lakehouse/gold/greenhouse_hourly_metrics"
    try:
        hourly_df.write.mode("overwrite").parquet(gold_hourly_path)
        print(f"[ANALYTICS] Persisted to Gold Lakehouse: {gold_hourly_path}")
    except Exception as e:
        print(f"[ANALYTICS] Notice saving Gold S3A: {e}. Writing local fallback.")
        hourly_df.write.mode("overwrite").parquet("./lakehouse/gold/greenhouse_hourly_metrics")

    # Persist Hourly Aggregates to PostgreSQL Serving Layer
    hourly_rows = hourly_df.collect()
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        for r in hourly_rows:
            cur.execute("""
                INSERT INTO greenhouse_hourly_metrics
                (greenhouse_id, window_start, window_end, avg_temperature, min_temperature, max_temperature,
                 avg_humidity, avg_soil_moisture, min_soil_moisture, max_soil_moisture, avg_light_lux, avg_co2, measurement_count, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (greenhouse_id, window_start) DO UPDATE SET
                    avg_temperature = EXCLUDED.avg_temperature,
                    min_temperature = EXCLUDED.min_temperature,
                    max_temperature = EXCLUDED.max_temperature,
                    avg_humidity = EXCLUDED.avg_humidity,
                    avg_soil_moisture = EXCLUDED.avg_soil_moisture,
                    min_soil_moisture = EXCLUDED.min_soil_moisture,
                    max_soil_moisture = EXCLUDED.max_soil_moisture,
                    avg_light_lux = EXCLUDED.avg_light_lux,
                    avg_co2 = EXCLUDED.avg_co2,
                    measurement_count = EXCLUDED.measurement_count,
                    updated_at = CURRENT_TIMESTAMP;
            """, (
                r["greenhouse_id"], r["window_start"], r["window_end"],
                r["avg_temperature"], r["min_temperature"], r["max_temperature"],
                r["avg_humidity"], r["avg_soil_moisture"], r["min_soil_moisture"], r["max_soil_moisture"],
                r["avg_light_lux"], r["avg_co2"], r["measurement_count"]
            ))

        # 2. Daily Aggregations
        daily_df = df_clean.withColumn("metric_date", to_date(col("event_time"))).groupBy(
            col("greenhouse_id"),
            col("metric_date")
        ).agg(
            spark_round(avg("temperature"), 2).alias("avg_temperature"),
            spark_round(min("temperature"), 2).alias("min_temperature"),
            spark_round(max("temperature"), 2).alias("max_temperature"),
            spark_round(avg("humidity"), 2).alias("avg_humidity"),
            spark_round(avg("soil_moisture"), 2).alias("avg_soil_moisture")
        )

        daily_rows = daily_df.collect()
        for dr in daily_rows:
            cur.execute("""
                INSERT INTO greenhouse_daily_metrics
                (greenhouse_id, metric_date, avg_temperature, min_temperature, max_temperature, avg_humidity, avg_soil_moisture, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (greenhouse_id, metric_date) DO UPDATE SET
                    avg_temperature = EXCLUDED.avg_temperature,
                    min_temperature = EXCLUDED.min_temperature,
                    max_temperature = EXCLUDED.max_temperature,
                    avg_humidity = EXCLUDED.avg_humidity,
                    avg_soil_moisture = EXCLUDED.avg_soil_moisture,
                    updated_at = CURRENT_TIMESTAMP;
            """, (
                dr["greenhouse_id"], dr["metric_date"],
                dr["avg_temperature"], dr["min_temperature"], dr["max_temperature"],
                dr["avg_humidity"], dr["avg_soil_moisture"]
            ))

        conn.commit()
        cur.close()
        conn.close()
        print("[ANALYTICS] Successfully synchronized aggregates to PostgreSQL Serving Layer!")
    except Exception as e:
        print(f"[ANALYTICS] Error saving to PostgreSQL: {e}")

if __name__ == "__main__":
    run_analytics()
