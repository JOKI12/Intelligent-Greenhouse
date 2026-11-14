"""
Realistic IoT Sensor Simulator for Intelligent Greenhouses.
Publishes telemetry to MQTT topics: serres/{greenhouse_id}/{sensor_id}/mesures
Simulates diurnal cycles, soil drying, simulated irrigation, and anomalous spikes.
"""

import os
import sys
import time
import json
import random
import math
import argparse
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

try:
    from simulator.greenhouse_config import GREENHOUSES, SIMULATION_SETTINGS
except ImportError:
    from greenhouse_config import GREENHOUSES, SIMULATION_SETTINGS


class GreenhouseSimulator:
    def __init__(self, broker_host="localhost", broker_port=1883, interval=3, seed=42, inject_anomalies=True):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.interval = interval
        self.inject_anomalies = inject_anomalies
        self.seed = seed
        random.seed(seed)

        # Dynamic state of each greenhouse
        self.state = {}
        for gh_id, cfg in GREENHOUSES.items():
            self.state[gh_id] = {
                "soil_moisture": cfg["base_soil_moisture"],
                "irrigating": False,
                "cycles": 0
            }

        # Initialize MQTT client
        self.client = mqtt.Client(client_id=f"greenhouse_sim_{os.getpid()}_{random.randint(100, 999)}")
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"[SIMULATOR] Connected successfully to MQTT Broker at {self.broker_host}:{self.broker_port}")
        else:
            print(f"[SIMULATOR] Connection failed with code {rc}")

    def _on_disconnect(self, client, userdata, rc):
        print(f"[SIMULATOR] Disconnected from MQTT Broker (code {rc}). Reconnecting...")

    def connect(self):
        retries = 10
        for i in range(retries):
            try:
                self.client.connect(self.broker_host, self.broker_port, keepalive=60)
                self.client.loop_start()
                return True
            except Exception as e:
                print(f"[SIMULATOR] Attempt {i+1}/{retries} failed to connect to {self.broker_host}:{self.broker_port}: {e}")
                time.sleep(2)
        return False

    def generate_measurement(self, gh_id, sensor_id):
        cfg = GREENHOUSES[gh_id]
        st = self.state[gh_id]

        now = datetime.now(timezone.utc)
        # Solar diurnal cycle (24h sinusoid)
        hour = now.hour + now.minute / 60.0 + now.second / 3600.0
        # Peak sun around 13:00 (1 PM)
        solar_factor = max(0.0, math.sin(math.pi * (hour - 6) / 14)) if 6 <= hour <= 20 else 0.0

        # Temperature
        temp = cfg["base_temp"] + (cfg["temp_amplitude"] * (solar_factor - 0.3)) + random.uniform(-0.5, 0.5)

        # Light lux (0 at night, up to light_max at noon with cloud noise)
        light = cfg["light_max"] * solar_factor * random.uniform(0.85, 1.05)

        # Humidity (inversely related to temperature)
        hum = cfg["base_humidity"] - (solar_factor * 15.0) + random.uniform(-2.0, 2.0)
        hum = max(15.0, min(99.0, hum))

        # Soil moisture dynamics:
        # Drying occurs when not irrigating, accelerated by light & temperature
        drying = cfg["moisture_decay_rate"] * (1.0 + solar_factor * 0.8) + random.uniform(-0.05, 0.05)
        
        if st["irrigating"]:
            # Irrigation is running: moisture recovers rapidly
            st["soil_moisture"] += 6.5 + random.uniform(-0.5, 0.5)
            if st["soil_moisture"] >= cfg["base_soil_moisture"] + 8.0:
                st["irrigating"] = False
                print(f"[SIMULATOR] [{gh_id}] Simulated irrigation cycle finished. Soil moisture restored.")
        else:
            st["soil_moisture"] -= drying
            if st["soil_moisture"] <= cfg["irrigation_threshold"]:
                # Trigger automatic simulated irrigation cycle
                st["irrigating"] = True
                print(f"[SIMULATOR] [{gh_id}] Soil moisture dropped to {st['soil_moisture']:.1f}%. Starting simulated irrigation cycle.")

        soil = max(10.0, min(95.0, st["soil_moisture"]))

        # CO2 (photosynthesis consumes CO2 during day, respiration releases it at night)
        co2 = cfg["base_co2"] - (solar_factor * 120.0) + random.uniform(-15.0, 15.0)

        status = "OK"

        # Occasional anomaly injection (5% probability if enabled)
        if self.inject_anomalies and random.random() < SIMULATION_SETTINGS["anomaly_probability"]:
            anomaly_type = random.choice(["heatwave", "dry_soil", "sensor_glitch"])
            if anomaly_type == "heatwave":
                temp += 12.0
                status = "WARNING_HIGH_TEMP"
            elif anomaly_type == "dry_soil":
                soil = max(5.0, soil - 20.0)
                status = "WARNING_LOW_MOISTURE"
            elif anomaly_type == "sensor_glitch":
                temp = -99.0
                status = "ERROR_SENSOR_FAULT"

        payload = {
            "greenhouse_id": gh_id,
            "sensor_id": sensor_id,
            "crop": cfg["crop"],
            "temperature": round(temp, 2),
            "humidity": round(hum, 2),
            "soil_moisture": round(soil, 2),
            "light_lux": round(light, 1),
            "co2": round(co2, 1),
            "status": status,
            "timestamp": now.isoformat()
        }
        return payload

    def run(self, max_iterations=None):
        print(f"[SIMULATOR] Starting telemetry loop (Interval={self.interval}s, Seed={self.seed})...")
        iteration = 0
        try:
            while max_iterations is None or iteration < max_iterations:
                for gh_id, cfg in GREENHOUSES.items():
                    for sensor_id in cfg["sensors"]:
                        payload = self.generate_measurement(gh_id, sensor_id)
                        topic = f"serres/{gh_id}/{sensor_id}/mesures"
                        msg_str = json.dumps(payload)
                        self.client.publish(topic, msg_str, qos=1)
                        # print subtle log
                        if iteration % 10 == 0 and sensor_id == "capteur_01":
                            print(f"[PUB] {topic} -> Temp: {payload['temperature']}°C, Soil: {payload['soil_moisture']}%, Status: {payload['status']}")
                iteration += 1
                time.sleep(self.interval)
        except KeyboardInterrupt:
            print("\n[SIMULATOR] Stopping simulator...")
        finally:
            self.client.loop_stop()
            self.client.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Greenhouse IoT Telemetry Simulator")
    parser.add_argument("--host", default=os.getenv("MQTT_BROKER_HOST", "localhost"), help="MQTT Broker host")
    parser.add_argument("--port", type=int, default=int(os.getenv("MQTT_BROKER_PORT", 1883)), help="MQTT Broker port")
    parser.add_argument("--interval", type=int, default=int(os.getenv("SIMULATOR_INTERVAL", 3)), help="Seconds between rounds")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--count", type=int, default=None, help="Max iterations (None for infinite)")
    args = parser.parse_args()

    sim = GreenhouseSimulator(
        broker_host=args.host,
        broker_port=args.port,
        interval=args.interval,
        seed=args.seed
    )
    if sim.connect():
        sim.run(max_iterations=args.count)
    else:
        print("[SIMULATOR] Error: Could not connect to MQTT broker.")
        sys.exit(1)
