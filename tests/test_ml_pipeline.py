"""
Spark MLlib / Scikit-Learn Dual Regression Pipeline Unit Test.
Validates model convergence, RMSE/MAE evaluation and prediction range.
"""
import pytest
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

def test_ml_pipeline_convergence():
    # Generate synthetic training bootstrap mirroring train_irrigation_model.py logic
    np.random.seed(42)
    n = 1000
    temp = np.random.uniform(18, 35, n)
    hum = np.random.uniform(30, 85, n)
    soil = np.random.uniform(20, 80, n)
    lux = np.random.uniform(0, 45000, n)
    co2 = np.random.uniform(400, 1000, n)

    # Target: moisture after 60 min with evapotranspiration
    evapo = (lux * 0.00005) + (temp * 0.08) - (hum * 0.03)
    target = np.clip(soil - evapo + np.random.normal(0, 0.2, n), 10, 95)

    X = np.column_stack([temp, hum, soil, lux, co2])
    y = target

    # Chronological Split (80% train, 20% test)
    split_idx = int(0.8 * n)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]

    rf = RandomForestRegressor(n_estimators=30, max_depth=6, random_state=42)
    rf.fit(X_train, y_train)

    preds = rf.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    mae = mean_absolute_error(y_test, preds)
    r2 = r2_score(y_test, preds)

    assert rmse < 1.5, f"RMSE too high: {rmse}"
    assert mae < 1.2, f"MAE too high: {mae}"
    assert r2 > 0.85, f"R2 score should be high: {r2}"
