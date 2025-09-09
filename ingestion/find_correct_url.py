from influxdb_client_3 import InfluxDBClient3
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

token = os.environ.get("INFLUXDB_TOKEN")
org = "Dev Team"

# Common InfluxDB Cloud URLs to try
urls_to_try = [
    "https://us-east-1-1.aws.cloud2.influxdata.com",
    "https://us-west-2-1.aws.cloud2.influxdata.com", 
    "https://eu-central-1-1.aws.cloud2.influxdata.com",
    "https://us-east-1-2.aws.cloud2.influxdata.com",
    "https://us-west-2-2.aws.cloud2.influxdata.com"
]

print(f"Testing connection with token: {token[:20]}...")
print(f"Organization: {org}")
print()

for url in urls_to_try:
    print(f"Testing: {url}")
    try:
        client = InfluxDBClient3(host=url, token=token, org=org)
        # Try a simple query
        databases = client.query(database="_internal", query="SHOW DATABASES")
        print(f"✅ SUCCESS! Found {len(list(databases))} databases")
        print(f"   Use this URL: {url}")
        break
    except Exception as e:
        print(f"❌ Failed: {str(e)[:100]}...")
        print()

print("\nIf none worked, please check your InfluxDB dashboard for the correct URL and organization name.")
