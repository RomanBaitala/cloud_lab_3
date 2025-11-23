import json
import time
import threading
import boto3
from random import uniform
from datetime import datetime, timezone
import argparse
import sys

sqs = boto3.client("sqs", region_name="us-east-1")


def load_config(path):
    """Load configuration file."""
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to load config: {e}")
        sys.exit(1)


def send_valid_data(queue_url, payload):
    """Send correct JSON IoT message to AWS SQS."""
    try:
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(payload)
        )
    except Exception as e:
        print(f"[ERROR] Valid SQS send failed: {e}")


def send_invalid_data(queue_url, sensor_type):
    """Send broken data (text instead of JSON) to trigger errors."""
    try:
        fake_payload = "THIS IS NOT A JSON AND WILL BREAK LAMBDA"
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=fake_payload
        )
        print(f"[TEST] Sent BROKEN data for {sensor_type}")
    except Exception as e:
        print(f"[ERROR] Invalid SQS send failed: {e}")


def sensor_worker(sensor_conf, queue_url):
    """Thread for a single sensor simulation."""

    device_id = sensor_conf.get("deviceId", "unknown")
    sensor_type = sensor_conf["type"]
    interval = sensor_conf["interval_ms"] / 1000.0

    location = sensor_conf["location"]

    print(f"[INFO] Started sensor: {sensor_type} (ID={device_id}), Loc={location}, Interval={interval}s")

    while True:
        try:
            value = round(uniform(sensor_conf["min_value"], sensor_conf["max_value"]), 2)

            payload = {
                "deviceId": device_id,
                "sensorType": sensor_type,
                "value": value,
                "unit": sensor_conf["unit"],
                "location": location,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

            if sensor_type == "humidity":
                send_invalid_data(queue_url, sensor_type)
            else:
                send_valid_data(queue_url, payload)

            # 4. Очікування
            time.sleep(interval)

        except Exception as e:
            print(f"Error in worker {sensor_type}: {e}")
            time.sleep(1)


def main(config_path):
    config = load_config(config_path)

    queue_url = config.get("queue_url")

    if not queue_url:
        print("[ERROR] 'queue_url' is missing in config file!")
        sys.exit(1)

    print("[INFO] Target SQS Queue:", queue_url)

    threads = []
    for sensor_conf in config["sensors"]:
        t = threading.Thread(
            target=sensor_worker,
            args=(sensor_conf, queue_url),
            daemon=True
        )
        threads.append(t)
        t.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[INFO] Simulation stopped by user.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IoT Sensor Emulator to AWS SQS")
    parser.add_argument("--config", default="config.json", help="Path to config.json")
    args = parser.parse_args()

    main(args.config)