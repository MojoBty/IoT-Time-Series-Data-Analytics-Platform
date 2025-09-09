"""
Data Archiving Service for IoT Time Series Platform
Handles periodic archiving of sensor data to Swift object storage
"""
import time
import threading
import csv
import io
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from swift_client import SwiftArchiver
from influxdb_client import InfluxDBClient, QueryApi
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataArchivingService:
    """
    Service for archiving IoT sensor data to Swift object storage
    """
    
    def __init__(self, 
                 archive_interval_hours: int = 1,
                 swift_archiver: Optional[SwiftArchiver] = None):
        """
        Initialize the archiving service
        
        Args:
            archive_interval_hours: How often to archive data (in hours)
            swift_archiver: Swift archiver instance (creates default if None)
        """
        self.archive_interval = archive_interval_hours * 3600  # Convert to seconds
        self.swift_archiver = swift_archiver or SwiftArchiver()
        self.running = False
        self.thread = None
        
        # InfluxDB configuration
        self.influx_token = os.environ.get("INFLUXDB_TOKEN")
        self.influx_org = "Dev Team"
        self.influx_host = "https://us-east-1-1.aws.cloud2.influxdata.com"
        self.influx_bucket = "Sensor Data"
        
        # Initialize InfluxDB client
        if self.influx_token:
            self.influx_client = InfluxDBClient(
                url=self.influx_host, 
                token=self.influx_token, 
                org=self.influx_org
            )
            self.query_api = self.query_api()
        else:
            logger.warning("No InfluxDB token found. Archiving will use in-memory data only.")
            self.influx_client = None
            self.query_api = None
    
    def start(self):
        """Start the archiving service in a background thread"""
        if self.running:
            logger.warning("Archiving service is already running")
            return
            
        # Connect to Swift
        if not self.swift_archiver.connect():
            logger.error("Failed to connect to Swift storage. Cannot start archiving service.")
            return
            
        self.running = True
        self.thread = threading.Thread(target=self._archive_loop, daemon=True)
        self.thread.start()
        logger.info(f"Data archiving service started (interval: {self.archive_interval/3600:.1f} hours)")
    
    def stop(self):
        """Stop the archiving service"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        logger.info("Data archiving service stopped")
    
    def _archive_loop(self):
        """Main archiving loop"""
        while self.running:
            try:
                # Calculate time range for archiving
                end_time = datetime.now()
                start_time = end_time - timedelta(hours=self.archive_interval/3600)
                
                # Archive data
                self._archive_data_range(start_time, end_time)
                
                # Wait for next archive cycle
                time.sleep(self.archive_interval)
                
            except Exception as e:
                logger.error(f"Error in archiving loop: {e}")
                time.sleep(60)  # Wait 1 minute before retrying
    
    def _archive_data_range(self, start_time: datetime, end_time: datetime):
        """
        Archive data for a specific time range
        
        Args:
            start_time: Start of time range
            end_time: End of time range
        """
        try:
            # Get data from InfluxDB
            data = self._get_influxdb_data(start_time, end_time)
            
            if not data:
                logger.info(f"No data found for range {start_time} to {end_time}")
                return
            
            # Archive as JSON
            timestamp = start_time.strftime("%Y%m%d_%H%M%S")
            json_filename = f"sensor_data_{timestamp}.json"
            self.swift_archiver.archive_sensor_data(data, json_filename)
            
            # Archive as CSV
            csv_content = self._convert_to_csv(data)
            csv_filename = f"sensor_data_{timestamp}.csv"
            self.swift_archiver.archive_csv_data(csv_content, csv_filename)
            
            logger.info(f"Archived {len(data)} records for {start_time} to {end_time}")
            
        except Exception as e:
            logger.error(f"Failed to archive data range: {e}")
    
    def _get_influxdb_data(self, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """
        Get sensor data from InfluxDB for the specified time range
        
        Args:
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            List of sensor data records
        """
        if not self.query_api:
            logger.warning("No InfluxDB connection available")
            return []
            
        try:
            # Build InfluxDB query
            start_iso = start_time.isoformat()
            end_iso = end_time.isoformat()
            
            query = f'''
            from(bucket: "{self.influx_bucket}")
            |> range(start: {start_iso}, stop: {end_iso})
            |> filter(fn: (r) => r._measurement == "sensors")
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> sort(columns: ["_time"])
            '''
            
            # Execute query
            tables = self.query_api.query(query=query)
            
            # Convert to list of dictionaries
            data = []
            for table in tables:
                for record in table.records:
                    data.append({
                        "timestamp": record.get_time().isoformat(),
                        "sensor_id": record.values.get("sensor_id"),
                        "temperature": record.values.get("temperature"),
                        "humidity": record.values.get("humidity"),
                        "cpu": record.values.get("cpu")
                    })
            
            return data
            
        except Exception as e:
            logger.error(f"Failed to query InfluxDB: {e}")
            return []
    
    def _convert_to_csv(self, data: List[Dict[str, Any]]) -> str:
        """
        Convert sensor data to CSV format
        
        Args:
            data: List of sensor data dictionaries
            
        Returns:
            CSV content as string
        """
        if not data:
            return ""
            
        # Create CSV in memory
        output = io.StringIO()
        fieldnames = ["timestamp", "sensor_id", "temperature", "humidity", "cpu"]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        
        # Write header
        writer.writeheader()
        
        # Write data rows
        for record in data:
            writer.writerow(record)
        
        return output.getvalue()
    
    def archive_manual(self, hours_back: int = 1) -> bool:
        """
        Manually trigger archiving for the last N hours
        
        Args:
            hours_back: Number of hours to archive
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours_back)
            
            self._archive_data_range(start_time, end_time)
            return True
            
        except Exception as e:
            logger.error(f"Manual archiving failed: {e}")
            return False
    
    def get_archive_status(self) -> Dict[str, Any]:
        """
        Get status information about the archiving service
        
        Returns:
            Dictionary with status information
        """
        try:
            # Get list of archived files
            files = self.swift_archiver.list_archived_files(limit=10)
            
            return {
                "running": self.running,
                "archive_interval_hours": self.archive_interval / 3600,
                "swift_connected": self.swift_archiver.conn is not None,
                "influxdb_connected": self.query_api is not None,
                "recent_files": files,
                "total_files": len(self.swift_archiver.list_archived_files(limit=1000))
            }
            
        except Exception as e:
            logger.error(f"Failed to get archive status: {e}")
            return {
                "running": self.running,
                "error": str(e)
            }

# Global archiving service instance
_archiving_service = None

def get_archiving_service() -> DataArchivingService:
    """Get the global archiving service instance"""
    global _archiving_service
    if _archiving_service is None:
        _archiving_service = DataArchivingService()
    return _archiving_service

def start_archiving_service():
    """Start the global archiving service"""
    service = get_archiving_service()
    service.start()

def stop_archiving_service():
    """Stop the global archiving service"""
    global _archiving_service
    if _archiving_service:
        _archiving_service.stop()
