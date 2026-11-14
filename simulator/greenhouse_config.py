"""
Configuration for the Intelligent Greenhouse simulation.
Defines parameters for 3 realistic agricultural greenhouses:
- Serre 01: Tomatoes (Hot & moderately humid)
- Serre 02: Cucumbers / Tropical (High humidity & steady heat)
- Serre 03: Strawberries / Berries (Moderate temp, sensitive soil moisture)
"""

GREENHOUSES = {
    "serre_01": {
        "name": "Serre Tomates A",
        "crop": "Tomatoes",
        "sensors": ["capteur_01", "capteur_02"],
        "base_temp": 24.0,
        "base_humidity": 65.0,
        "base_soil_moisture": 60.0,
        "base_co2": 550.0,
        "temp_amplitude": 7.0,
        "light_max": 45000.0,
        "moisture_decay_rate": 0.45,
        "irrigation_threshold": 30.0,
        "critical_moisture_threshold": 25.0,
        "critical_temp_max": 35.0,
        "critical_humidity_min": 30.0,
        "critical_co2_max": 1200.0
    },
    "serre_02": {
        "name": "Serre Concombres B",
        "crop": "Cucumbers",
        "sensors": ["capteur_01", "capteur_02"],
        "base_temp": 26.5,
        "base_humidity": 80.0,
        "base_soil_moisture": 70.0,
        "base_co2": 620.0,
        "temp_amplitude": 5.5,
        "light_max": 38000.0,
        "moisture_decay_rate": 0.55,
        "irrigation_threshold": 35.0,
        "critical_moisture_threshold": 28.0,
        "critical_temp_max": 36.0,
        "critical_humidity_min": 35.0,
        "critical_co2_max": 1300.0
    },
    "serre_03": {
        "name": "Serre Fraises C",
        "crop": "Strawberries",
        "sensors": ["capteur_01", "capteur_02"],
        "base_temp": 20.0,
        "base_humidity": 55.0,
        "base_soil_moisture": 50.0,
        "base_co2": 480.0,
        "temp_amplitude": 8.0,
        "light_max": 50000.0,
        "moisture_decay_rate": 0.35,
        "irrigation_threshold": 25.0,
        "critical_moisture_threshold": 20.0,
        "critical_temp_max": 32.0,
        "critical_humidity_min": 25.0,
        "critical_co2_max": 1100.0
    }
}

SIMULATION_SETTINGS = {
    "default_publish_interval_seconds": 3,
    "anomaly_probability": 0.05,
    "random_seed": 42
}
