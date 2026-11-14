import pytest
from datetime import datetime
from simulator.greenhouse_config import GREENHOUSES, SIMULATION_SETTINGS
from simulator.sensor_simulator import GreenhouseSimulator
from kafka_gateway.mqtt_kafka_bridge import MqttKafkaBridge

def test_greenhouse_config_definitions():
    """Verify greenhouse configurations are complete and physically sound"""
    assert "serre_01" in GREENHOUSES
    assert "serre_02" in GREENHOUSES
    assert "serre_03" in GREENHOUSES
    for gh_id, cfg in GREENHOUSES.items():
        assert "crop" in cfg
        assert "sensors" in cfg
        assert len(cfg["sensors"]) >= 2
        assert cfg["irrigation_threshold"] > 0
        assert cfg["critical_temp_max"] > 30.0

def test_simulator_measurement_generation():
    """Verify IoT simulator telemetry generation and ranges"""
    sim = GreenhouseSimulator(seed=123, inject_anomalies=False)
    m = sim.generate_measurement("serre_01", "capteur_01")
    assert m["greenhouse_id"] == "serre_01"
    assert m["sensor_id"] == "capteur_01"
    assert 10.0 <= m["temperature"] <= 50.0
    assert 15.0 <= m["humidity"] <= 100.0
    assert 10.0 <= m["soil_moisture"] <= 95.0
    assert m["light_lux"] >= 0.0
    assert m["status"] == "OK"

def test_bridge_validation():
    """Verify MQTT-to-Kafka schema validation and Dead-Letter Queue criteria"""
    bridge = MqttKafkaBridge()
    valid_payload = {
        "greenhouse_id": "serre_01",
        "sensor_id": "capteur_01",
        "temperature": 25.5,
        "humidity": 60.0,
        "soil_moisture": 45.0,
        "timestamp": datetime.utcnow().isoformat()
    }
    is_valid, reason = bridge.validate_message(valid_payload)
    assert is_valid is True

    # Test out of physical bound temperature (> 90°C)
    invalid_payload = valid_payload.copy()
    invalid_payload["temperature"] = 150.0
    is_valid, reason = bridge.validate_message(invalid_payload)
    assert is_valid is False
    assert "Temperature out of plausible physical range" in reason

    # Test missing mandatory field
    missing_payload = valid_payload.copy()
    del missing_payload["soil_moisture"]
    is_valid, reason = bridge.validate_message(missing_payload)
    assert is_valid is False
    assert "Missing required field" in reason

def test_alert_rules_consistency():
    """Verify alert threshold values prevent erroneous triggers"""
    from spark.config.alert_rules import ALERT_THRESHOLDS
    assert ALERT_THRESHOLDS["temperature"]["max"] == 35.0
    assert ALERT_THRESHOLDS["soil_moisture"]["min_critical"] == 22.0
    assert ALERT_THRESHOLDS["humidity"]["min"] == 30.0
