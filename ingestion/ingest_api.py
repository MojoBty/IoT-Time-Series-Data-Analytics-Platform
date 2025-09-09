from flask import Flask, request, jsonify
import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# InfluxDB client (2.x)
token = os.environ.get("INFLUXDB_TOKEN")
org = "Dev Team"
host = "https://us-east-1-1.aws.cloud2.influxdata.com"
bucket = "Sensor Data"  # In InfluxDB 2.x, it's called "bucket" not "database"

client = InfluxDBClient(url=host, token=token, org=org)
write_api = client.write_api()
query_api = client.query_api()

sensor_data = []

@app.route("/ingest", methods=['POST'])
def ingest_data():
  """Endpoint to ingest sensor data"""
  try:
    data = request.json
    print(f"Received data: {data}")
    
    # Validate required fields
    required_fields = ["sensor_id", "timestamp", "temperature", "humidity", "cpu"]
    for field in required_fields:
      if field not in data:
        return jsonify({"error": f"Missing required field: {field}"}), 400

    # Add data to list of sensor data (this is where we would store the data in a database)
    sensor_data.append(data)
    
    # Create InfluxDB point (2.x syntax)
    point = (
    Point("sensors")
    .tag("sensor_id", data["sensor_id"])
    .field("temperature", data["temperature"])
    .field("humidity", data["humidity"])
    .field("cpu", data["cpu"])
    .time(data["timestamp"], WritePrecision.NS)
    )
    
    # Write to InfluxDB 2.x
    try:
      write_api.write(bucket=bucket, record=point)
      print(f"[{datetime.datetime.now()}] Data written to InfluxDB: {data}")
    except Exception as db_error:
      print(f"[{datetime.datetime.now()}] Error writing to InfluxDB: {db_error}")
      # Continue processing even if DB write fails
      pass

    print(f"[{datetime.datetime.now()}] Received: {data}")
    return jsonify({"status": "ok"}), 200
  except Exception as e:
    return jsonify({"error": str(e)}), 500
    
@app.route("/data", methods=["GET"])
def get_data():
  """Endpoint to view what we've collected so far"""
  # Return last 50 readings
  return jsonify(sensor_data[-50:])

@app.route("/influxdb-data", methods=["GET"])
def get_influxdb_data():
  """Endpoint to query data from InfluxDB"""
  try:
    # Query InfluxDB 2.x
    query = f'from(bucket: "{bucket}") |> range(start: -1h) |> filter(fn: (r) => r._measurement == "sensors") |> limit(n: 50)'
    tables = query_api.query(query=query)
    
    data = []
    for table in tables:
      for record in table.records:
        data.append({
          "time": record.get_time().isoformat(),
          "sensor_id": record.values.get("sensor_id"),
          "field": record.get_field(),
          "value": record.get_value()
        })
    
    return jsonify({"data": data, "count": len(data)})
  except Exception as e:
    return jsonify({"error": str(e)}), 500  

if __name__ == "__main__":
  app.run(host="0.0.0.0", port=5000, debug=True)
