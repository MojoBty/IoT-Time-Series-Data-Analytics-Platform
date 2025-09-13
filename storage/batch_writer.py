"""
Hourly Batch Writer:
Queries InfluxDB for the last hour of sensor data,
saves to Parquet, and archives to Swift.
"""

import datetime
import io
import pandas as pd
import os
from influxdb_client import InfluxDBClient
from swift_client import get_swift_connection, ensure_container_exists  # import your Swift helper

# InfluxDB client (2.x)
token = os.environ.get("INFLUXDB_TOKEN")
org = "Dev Team"
host = "https://us-east-1-1.aws.cloud2.influxdata.com"
bucket = "Sensor Data"

client = InfluxDBClient(url=host, token=token, org=org)
write_api = client.write_api()
query_api = client.query_api()

SWIFT_CONTAINER = "sensor-archive"

# Query InfluxDB (last 1 hour of data)
def query_last_hour():
  query = f'from(bucket: "{bucket}") |> range(start: -1h) |> filter(fn: (r) => r._measurement == "sensors") |> limit(n: 50)'

  # Reshape Influx data into a DataFrame
  try:
    with InfluxDBClient(url=host, token=token, org=org) as client:
      result = client.query_api().query(query=query)
      records = []
      for table in result:
        for record in table.records:
          records.append({
            "time": record.get_time(), 
            "sensor_id": record.values.get("sensor_id"), 
            record.get_field(): record.get_value()
          })
      return pd.DataFrame(records)
  except Exception as e:
    print(f"Error querying InfluxDB: {e}")
    return pd.DataFrame()

#  Save to Parquet buffer
def dataframe_to_parquet(df):
  buf = io.BytesIO()
  df.to_parquet(buf, index=False, compression="snappy")
  buf.seek(0)
  return buf

# Upload to Swift
def upload_to_swift(buf, timestamp):
  conn = get_swift_connection()  
  ensure_container_exists(conn, SWIFT_CONTAINER)
  object_name = f"hourly/{timestamp}.parquet"

  conn.put_object(
    SWIFT_CONTAINER,
    object_name,
    contents=buf.read(),
    content_type="application/octet-stream"
  )
  print(f"Uploaded {object_name} to Swift")

if __name__ == "__main__":
  df = query_last_hour()
  if not df.empty:
      buf = dataframe_to_parquet(df)
      ts_str = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
      upload_to_swift(buf, ts_str)
  else:
      print("⚠️ No data found for the last hour.")