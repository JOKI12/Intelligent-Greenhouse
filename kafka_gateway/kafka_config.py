import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
KAFKA_TOPIC_MEASURES = os.getenv("KAFKA_TOPIC_MEASURES", "mesures_serre")
KAFKA_TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", "mesures_serre_dlq")

MQTT_BROKER = os.getenv("MQTT_BROKER_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))
MQTT_TOPIC_FILTER = "serres/+/+/mesures"
