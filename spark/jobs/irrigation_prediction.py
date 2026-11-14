"""
Spark MLlib Predictive Inference & Decision Support:
1. Loads the latest trained RandomForest model.
2. Reads current measurements from PostgreSQL 'latest_measurements'.
3. Predicts 'soil_moisture_next_60min'.
4. Evaluates irrigation requirement based on crop-specific thresholds.
5. Saves predictions & irrigation recommendations to PostgreSQL table 'irrigation_predictions'.
"""

import os
import sys
import psycopg2
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

try:
    from spark.config.spark_config import get_spark_session
    from simulator.greenhouse_config import GREENHOUSES
except ImportError:
    from config.spark_config import get_spark_session
    try:
        from simulator.greenhouse_config import GREENHOUSES
    except ImportError:
        GREENHOUSES = {
            "serre_01": {"irrigation_threshold": 30.0},
            "serre_02": {"irrigation_threshold": 35.0},
            "serre_03": {"irrigation_threshold": 25.0}
        }

def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "greenhouse_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres")
    )

def run_predictions():
    print("[ML-PREDICT] Running Predictive Irrigation Inference...")
    spark = get_spark_session(app_name="PredictiveIrrigationInference")
    spark.sparkContext.setLogLevel("WARN")

    model_dir = "./spark/models/rf_irrigation_model"
    if not os.path.exists(model_dir):
        print(f"[ML-PREDICT] Error: Model not found at {model_dir}. Please run train_irrigation_model.py first.")
        return

    try:
        model = PipelineModel.load(model_dir)
        print(f"[ML-PREDICT] Loaded trained model from {model_dir}")
    except Exception as e:
        print(f"[ML-PREDICT] Failed to load model: {e}")
        return

    # Fetch latest greenhouse measurements from PostgreSQL
    pg_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', 5432)}/{os.getenv('POSTGRES_DB', 'greenhouse_db')}"
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", pg_url) \
            .option("dbtable", "(SELECT DISTINCT ON (greenhouse_id) * FROM latest_measurements ORDER BY greenhouse_id, timestamp DESC) as latest_gh") \
            .option("user", os.getenv("POSTGRES_USER", "postgres")) \
            .option("password", os.getenv("POSTGRES_PASSWORD", "postgres")) \
            .option("driver", "org.postgresql.Driver") \
            .load()
    except Exception as e:
        print(f"[ML-PREDICT] Error reading latest measurements from DB: {e}. Generating default test points.")
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        schema = StructType([
            StructField("greenhouse_id", StringType(), False),
            StructField("temperature", DoubleType(), False),
            StructField("humidity", DoubleType(), False),
            StructField("soil_moisture", DoubleType(), False),
            StructField("light_lux", DoubleType(), False),
            StructField("co2", DoubleType(), False)
        ])
        df = spark.createDataFrame([
            ("serre_01", 28.5, 55.0, 29.5, 35000.0, 600.0),
            ("serre_02", 26.0, 75.0, 36.2, 28000.0, 620.0),
            ("serre_03", 22.0, 50.0, 24.1, 40000.0, 510.0)
        ], schema)

    if df.count() == 0:
        print("[ML-PREDICT] No measurements found to predict.")
        return

    # Predict
    preds_df = model.transform(df)
    results = preds_df.select("greenhouse_id", "soil_moisture", "prediction").collect()

    now_ts = datetime.now(timezone.utc)
    try:
        conn = get_pg_connection()
        cur = conn.cursor()

        for r in results:
            gh_id = r["greenhouse_id"]
            current_moisture = float(r["soil_moisture"])
            predicted_60min = round(float(r["prediction"]), 2)

            threshold = GREENHOUSES.get(gh_id, {}).get("irrigation_threshold", 30.0)
            recommend = predicted_60min <= threshold

            if recommend:
                msg = f"Soil moisture predicted to drop to {predicted_60min}% (threshold: {threshold}%). Irrigation recommended within next 60 minutes."
            else:
                msg = f"Moisture level healthy (predicted: {predicted_60min}% vs threshold: {threshold}%). No immediate irrigation needed."

            cur.execute("""
                INSERT INTO irrigation_predictions
                (greenhouse_id, timestamp, current_soil_moisture, predicted_moisture_60min, irrigation_recommended, recommendation_message, model_version)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (gh_id, now_ts, current_moisture, predicted_60min, recommend, msg, "v1.0.0"))

            print(f"[ML-PREDICT] [{gh_id}] Current: {current_moisture:.1f}% -> In 60m: {predicted_60min}% | Irrigation: {recommend}")

        conn.commit()
        cur.close()
        conn.close()
        print("[ML-PREDICT] Predictions and irrigation recommendations persisted to PostgreSQL!")
    except Exception as e:
        print(f"[ML-PREDICT] Error saving predictions to DB: {e}")

if __name__ == "__main__":
    run_predictions()
