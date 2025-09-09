"""
Swift Object Storage Client
"""
import swiftclient
import json
from datetime import datetime

def get_swift_connection():
    """Get a Swift connection with default test credentials"""
    return swiftclient.Connection(
        authurl="http://localhost:8080/auth/v1.0",
        user="test:tester",
        key="testing",
        auth_version="1"
    )

def ensure_container_exists(conn, container_name="sensor-archive"):
    """Ensure the container exists, create if it doesn't"""
    try:
        conn.head_container(container_name)
        print(f"Container '{container_name}' already exists")
    except swiftclient.exceptions.ClientException:
        conn.put_container(container_name)
        print(f"Created container '{container_name}'")

def archive_sensor_data(data, filename=None):
    """Archive sensor data as JSON"""
    conn = get_swift_connection()
    ensure_container_exists(conn)
    
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"sensor_data_{timestamp}.json"
    
    json_data = json.dumps(data, indent=2, default=str)
    conn.put_object("sensor-archive", filename, contents=json_data.encode('utf-8'))
    print(f"Archived {len(data)} records to {filename}")

def archive_csv_data(csv_content, filename=None):
    """Archive CSV data"""
    conn = get_swift_connection()
    ensure_container_exists(conn)
    
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"sensor_data_{timestamp}.csv"
    
    conn.put_object("sensor-archive", filename, contents=csv_content.encode('utf-8'))
    print(f"Archived CSV data to {filename}")

def list_archived_files():
    """List all archived files"""
    conn = get_swift_connection()
    _, objects = conn.get_container("sensor-archive")
    return objects

def download_file(filename):
    """Download a file from Swift"""
    conn = get_swift_connection()
    _, content = conn.get_object("sensor-archive", filename)
    return content

def delete_file(filename):
    """Delete a file from Swift"""
    conn = get_swift_connection()
    conn.delete_object("sensor-archive", filename)
    print(f"Deleted {filename}")

# Example usage:
if __name__ == "__main__":
    # Create connection
    conn = get_swift_connection()
    
    # Create container
    conn.put_container("sensor-archive")
    
    # Upload a test file
    conn.put_object("sensor-archive", "test.txt", contents=b"hello world")
    
    # List objects
    print("Files in container:")
    print(conn.get_container("sensor-archive")[1])
