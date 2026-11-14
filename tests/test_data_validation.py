import os
import json
import random
from datetime import datetime, timezone
from simulator.greenhouse_config import GREENHOUSES

def test_data_generation_e2e_schema():
    from simulator.sensor_simulator import GreenhouseSimulator
    sim = GreenhouseSimulator(seed=999, inject_anomalies=False)
    
    for gh_id in ["serre_01", "serre_02", "serre_03"]:
        payload = sim.generate_measurement(gh_id, "capteur_01")
        # Ensure serialization works flawlessly
        json_str = json.dumps(payload)
        parsed = json.loads(json_str)
        assert parsed["greenhouse_id"] == gh_id
        assert "temperature" in parsed
        assert "soil_moisture" in parsed
        assert "humidity" in parsed
        assert "light_lux" in parsed
        assert "co2" in parsed
        assert "timestamp" in parsed

def test_anomaly_injection():
    from simulator.sensor_simulator import GreenhouseSimulator
    sim = GreenhouseSimulator(seed=42, inject_anomalies=True)
    anomalies_detected = 0
    for _ in range(200):
        m = sim.generate_measurement("serre_01", "capteur_01")
        if m["status"] != "OK":
            anomalies_detected += 1
    assert anomalies_detected > 0, "Anomaly generator should generate at least one anomalous reading in 200 samples"
