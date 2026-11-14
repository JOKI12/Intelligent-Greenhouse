"""
Airflow DAG: Machine Learning Model Retraining & Inference.
Loads training data, trains Spark MLlib model, records evaluation metrics,
and triggers predictive irrigation inference.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'greenhouse_ml_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ml_irrigation_training',
    default_args=default_args,
    description='Train and evaluate Spark MLlib soil moisture model and generate predictions',
    schedule_interval='@weekly',
    catchup=False,
    tags=['greenhouse', 'machine_learning', 'mllib']
) as dag:

    train_mllib_model = BashOperator(
        task_id='train_and_evaluate_mllib_model',
        bash_command='python /opt/spark-apps/spark/jobs/train_irrigation_model.py'
    )

    run_predictions_task = BashOperator(
        task_id='generate_irrigation_predictions',
        bash_command='python /opt/spark-apps/spark/jobs/irrigation_prediction.py'
    )

    train_mllib_model >> run_predictions_task
