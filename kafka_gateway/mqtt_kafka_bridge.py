"""
MQTT to Kafka Bridge Gateway.
Subscribes to Mosquitto MQTT topics (serres/+/+/mesures),
Validates and enriches payloads with ingestion timestamp,
Publishes valid messages to Kafka topic 'mesures_serre',
Routes corrupted or invalid messages to Dead-Letter Queue 'mesures_serre_dlq'.
"""

import os
import sys
import json
import time
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

try:
    from kafka_gateway.kafka_config import KAFKA_BROKER, KAFKA_TOPIC_MEASURES, KAFKA_TOPIC_DLQ, MQTT_BROKER, MQTT_PORT, MQTT_TOPIC_FILTER
except ImportError:
    from kafka_config import KAFKA_BROKER, KAFKA_TOPIC_MEASURES, KAFKA_TOPIC_DLQ, MQTT_BROKER, MQTT_PORT, MQTT_TOPIC_FILTER


class MqttKafkaBridge:
    def __init__(self):
        self.kafka_broker = os.getenv("KAFKA_BROKER", KAFKA_BROKER)
        self.mqtt_broker = os.getenv("MQTT_BROKER_HOST", MQTT_BROKER)
        self.mqtt_port = int(os.getenv("MQTT_BROKER_PORT", MQTT_PORT))
        self.topic_measures = os.getenv("KAFKA_TOPIC_MEASURES", KAFKA_TOPIC_MEASURES)
        self.topic_dlq = os.getenv("KAFKA_TOPIC_DLQ", KAFKA_TOPIC_DLQ)

        self.producer = None
        self.mqtt_client = mqtt.Client(client_id="mqtt_kafka_bridge")
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_message = self._on_mqtt_message

    def init_kafka_producer(self, retries=15, delay=3):
        print(f"[BRIDGE] Connecting to Kafka at {self.kafka_broker}...")
        for i in range(retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_broker,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    retries=5,
                    acks='all'
                )
                print("[BRIDGE] Kafka Producer connected successfully!")
                return True
            except NoBrokersAvailable:
                print(f"[BRIDGE] Kafka broker not ready. Retry {i+1}/{retries} in {delay}s...")
                time.sleep(delay)
            except Exception as e:
                print(f"[BRIDGE] Unexpected error connecting to Kafka: {e}. Retrying...")
                time.sleep(delay)
        return False

    def _on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"[BRIDGE] Connected to MQTT broker at {self.mqtt_broker}:{self.mqtt_port}")
            self.mqtt_client.subscribe(MQTT_TOPIC_FILTER)
            print(f"[BRIDGE] Subscribed to topic: {MQTT_TOPIC_FILTER}")
        else:
            print(f"[BRIDGE] MQTT connection failed with code {rc}")

    def validate_message(self, raw_data):
        """
        Validates telemetry schema.
        Required: greenhouse_id, sensor_id, temperature, humidity, soil_moisture, timestamp.
        Ranges: temperature (-40 to 80), humidity (0 to 100), soil_moisture (0 to 100).
        """
        required = ["greenhouse_id", "sensor_id", "temperature", "humidity", "soil_moisture", "timestamp"]
        for field in required:
            if field not in raw_data:
                return False, f"Missing required field: {field}"

        t = raw_data["temperature"]
        h = raw_data["humidity"]
        s = raw_data["soil_moisture"]

        if not (-50.0 <= t <= 90.0):
            return False, f"Temperature out of plausible physical range: {t}"
        if not (0.0 <= h <= 100.0):
            return False, f"Humidity out of range: {h}"
        if not (0.0 <= s <= 100.0):
            return False, f"Soil moisture out of range: {s}"

        return True, "Valid"

    def _on_mqtt_message(self, client, userdata, msg):
        now_str = datetime.now(timezone.utc).isoformat()
        try:
            payload_str = msg.payload.decode('utf-8')
            raw_data = json.loads(payload_str)

            is_valid, reason = self.validate_message(raw_data)

            if is_valid:
                # Add ingestion metadata
                raw_data["ingestion_timestamp"] = now_str
                raw_data["mqtt_topic"] = msg.topic
                key = f"{raw_data['greenhouse_id']}_{raw_data['sensor_id']}"
                self.producer.send(self.topic_measures, key=key, value=raw_data)
            else:
                # Send to DLQ
                dlq_record = {
                    "raw_payload": payload_str,
                    "rejection_reason": reason,
                    "mqtt_topic": msg.topic,
                    "failed_at": now_str
                }
                self.producer.send(self.topic_dlq, key=msg.topic, value=dlq_record)
                print(f"[BRIDGE-DLQ] Routed invalid message to {self.topic_dlq}: {reason}")

        except Exception as e:
            dlq_record = {
                "raw_payload": str(msg.payload),
                "rejection_reason": f"Corrupted JSON payload or decoding error: {str(e)}",
                "mqtt_topic": msg.topic,
                "failed_at": now_str
            }
            if self.producer:
                self.producer.send(self.topic_dlq, key=msg.topic, value=dlq_record)
            print(f"[BRIDGE-DLQ] Corrupted payload routed to DLQ: {e}")

    def run(self):
        if not self.init_kafka_producer():
            print("[BRIDGE] Fatal: Could not establish Kafka connection.")
            sys.exit(1)

        print(f"[BRIDGE] Connecting to MQTT broker at {self.mqtt_broker}:{self.mqtt_port}...")
        self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, 60)
        print("[BRIDGE] Running MQTT-to-Kafka Bridge...")
        try:
            self.mqtt_client.loop_forever()
        except KeyboardInterrupt:
            print("\n[BRIDGE] Stopping bridge...")
        finally:
            if self.producer:
                self.producer.flush()
                self.producer.close()
            self.mqtt_client.disconnect()


if __name__ == "__main__":
    bridge = MqttKafkaBridge()
    bridge.run()
