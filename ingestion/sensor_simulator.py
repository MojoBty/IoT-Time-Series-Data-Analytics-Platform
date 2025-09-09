import time
import random
import datetime
import requests

# Ingest API URL
API_URL = "http://localhost:5000/ingest"

# List of sensors simulated
SENSORS = ["sensor-1", "sensor-2", "sensor-3"]

def generate_sensor_data(sensor_id):
  """Simulate one reading for a sensor"""
  return {
    "sensor_id": sensor_id,
    "timestamp": datetime.datetime.now().isoformat(),
    "temperature": round(20 + random.uniform(-3, 3), 2), # ~20 ± 3°C
    "humidity": round(45 + random.uniform(-5, 5), 2), # ~45 ± 5%
    "cpu": round(random.uniform(0.0, 1.0), 2), # 0.0–1.0 (normalized load)
  }

def main():
  """Main loop to generate and send sensor data"""
  while True:
    for sensor in SENSORS:
      # Send data to ingestion API
      payload = generate_sensor_data(sensor)

      try:
        response = requests.post(API_URL, json=payload)
        print(f"[{sensor}] Sent: {payload} | Status: {response.status_code}")
      except Exception as e:
        # Catch any errors
        print(f"[{sensor}] Error: {e}")
    # Sleep for 1 second (simulate 1hz)
    time.sleep(1)
  
if __name__ == "__main__":
  main()