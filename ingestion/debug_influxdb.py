from influxdb_client_3 import InfluxDBClient3
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# InfluxDB connection
token = os.environ.get("INFLUXDB_TOKEN")
org = "Dev Team"
host = "https://us-east-1-1.aws.cloud2.influxdata.com"
database = "Sensor Data"

if not token:
    print("ERROR: INFLUXDB_TOKEN not set!")
    exit(1)

try:
    client = InfluxDBClient3(host=host, token=token, org=org)
    print(f"Connected to InfluxDB at {host}")
    
    # Query 1: List all measurements
    print("\n=== MEASUREMENTS ===")
    measurements_query = "SHOW MEASUREMENTS"
    measurements = client.query(database=database, query=measurements_query)
    print("Measurements found:")
    for row in measurements:
        print(f"  - {row}")
    
    # Query 2: Get recent data from sensors measurement
    print("\n=== RECENT SENSOR DATA ===")
    recent_data_query = """
    SELECT * FROM sensors 
    ORDER BY time DESC 
    LIMIT 10
    """
    recent_data = client.query(database=database, query=recent_data_query)
    print("Recent sensor data:")
    for row in recent_data:
        print(f"  {row}")
    
    # Query 3: Count total records
    print("\n=== DATA COUNT ===")
    count_query = "SELECT COUNT(*) FROM sensors"
    count_result = client.query(database=database, query=count_query)
    print("Total records in sensors measurement:")
    for row in count_result:
        print(f"  {row}")
        
    # Query 4: Check time range
    print("\n=== TIME RANGE ===")
    time_range_query = """
    SELECT MIN(time) as earliest, MAX(time) as latest 
    FROM sensors
    """
    time_range = client.query(database=database, query=time_range_query)
    print("Data time range:")
    for row in time_range:
        print(f"  Earliest: {row.get('earliest', 'N/A')}")
        print(f"  Latest: {row.get('latest', 'N/A')}")

except Exception as e:
    print(f"Error: {e}")
