import pandas as pd
import io
import sys
import os

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from storage.swift_client import get_swift_connection


def load_day_from_swift(date_str):
    """
    Load all hourly Parquet files for a given day (YYYYMMDD) into a single DataFrame
    """
    conn = get_swift_connection()
    _, objects = conn.get_container("sensor-archive")

    # Filter for parquet files from the given day
    parquet_files = [obj['name'] for obj in objects if obj['name'].startswith(f"hourly/{date_str}")]

    dfs = []
    for fname in parquet_files:
        _, content = conn.get_object("sensor-archive", fname)
        dfs.append(pd.read_parquet(io.BytesIO(content)))

    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()  # empty if no files found