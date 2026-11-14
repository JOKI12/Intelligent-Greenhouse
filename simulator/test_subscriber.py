"""
MQTT subscriber test client for testing telemetry reception.
"""
import os
import paho.mqtt.client as mqtt

def on_connect(client, userdata, flags, rc):
    print(f"[TEST SUB] Connected to MQTT broker with rc={rc}")
    client.subscribe("serres/+/+/mesures")
    print("[TEST SUB] Subscribed to serres/+/+/mesures")

def on_message(client, userdata, msg):
    print(f"[TEST RECV] Topic: {msg.topic} -> Payload: {msg.payload.decode('utf-8')[:100]}...")

if __name__ == "__main__":
    host = os.getenv("MQTT_BROKER_HOST", "localhost")
    port = int(os.getenv("MQTT_BROKER_PORT", 1883))
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(host, port, 60)
    print(f"[TEST SUB] Listening on {host}:{port}...")
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n[TEST SUB] Stopping...")
