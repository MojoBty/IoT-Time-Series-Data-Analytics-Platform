"""
Generate historical sensor data using the same method as sensor_simulator.py
"""
import pandas as pd
import random
import datetime
import sys
import os

# Add the storage directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'storage'))
from swift_client import archive_parquet_data

def generate_sensor_data(sensor_id, timestamp):
    """Generate sensor data using the same method as sensor_simulator.py"""
    return {
        "sensor_id": sensor_id,
        "time": timestamp,
        "temperature": round(20 + random.uniform(-3, 3), 2),  # ~20 ± 3°C
        "humidity": round(45 + random.uniform(-5, 5), 2),     # ~45 ± 5%
        "cpu": round(random.uniform(0.0, 1.0), 2),           # 0.0–1.0 (normalized load)
    }

def generate_historical_data(days=30, interval_minutes=5):
    """
    Generate historical data using the same method as sensor_simulator.py
    
    Args:
        days: Number of days to generate data for
        interval_minutes: Time interval between readings
    """
    
    # Create time series
    start_time = datetime.datetime(2024, 1, 1, 0, 0, 0)
    end_time = start_time + datetime.timedelta(days=days)
    
    # Generate timestamps
    timestamps = pd.date_range(
        start=start_time, 
        end=end_time, 
        freq=f'{interval_minutes}T'
    )
    
    data = []
    sensors = ["sensor-1", "sensor-2", "sensor-3"]
    
    for timestamp in timestamps:
        for sensor in sensors:
            data.append(generate_sensor_data(sensor, timestamp))
    
    return pd.DataFrame(data)

def main():
    """Generate and archive historical data using original simulator method"""
    
    print("Generating historical data using original sensor_simulator method...")
    
    # Generate 30 days of data with 5-minute intervals
    df = generate_historical_data(days=30, interval_minutes=5)
    
    print(f"Generated {len(df)} records")
    print(f"Time range: {df['time'].min()} to {df['time'].max()}")
    print(f"Sensors: {df['sensor_id'].unique()}")
    print("\nSample data:")
    print(df.head(10))
    
    # Archive to Swift
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"historical_data_{timestamp}.parquet"
    
    archive_parquet_data(df, filename)
    print(f"\nArchived historical data to: {filename}")
    
    # Show some statistics
    print("\nData Statistics:")
    print(f"Total records: {len(df)}")
    print(f"Records per sensor: {len(df) // len(df['sensor_id'].unique())}")
    print(f"Temperature range: {df['temperature'].min():.1f}°C to {df['temperature'].max():.1f}°C")
    print(f"Humidity range: {df['humidity'].min():.1f}% to {df['humidity'].max():.1f}%")
    print(f"CPU range: {df['cpu'].min():.2f} to {df['cpu'].max():.2f}")
    
    return df, filename

if __name__ == "__main__":
    df, filename = main()
