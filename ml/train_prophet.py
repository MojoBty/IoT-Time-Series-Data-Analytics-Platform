import pandas as pd
from prophet import Prophet
import matplotlib.pyplot as plt
import sys
import os
import io

# Add the storage directory to the path to import swift_client
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'storage'))
from swift_client import download_file, list_archived_files

# Download parquet file from Swift storage
def download_parquet_from_swift(filename):
    """Download a parquet file from Swift and return as DataFrame"""
    try:
        # Download the file content
        content = download_file(filename)
        
        # Convert bytes to DataFrame
        buf = io.BytesIO(content)
        df = pd.read_parquet(buf)
        return df
    except Exception as e:
        print(f"Error downloading {filename}: {e}")
        return None

# List available parquet files
print("Available parquet files:")
files = list_archived_files()
parquet_files = [f['name'] for f in files if f['name'].endswith('.parquet')]
for i, file in enumerate(parquet_files):
    print(f"{i}: {file}")

# Download the most recent parquet file (or specify a filename)
if parquet_files:
    # Look for historical data files first, then training data, then any parquet file
    historical_files = [f for f in parquet_files if 'historical_data' in f]
    training_files = [f for f in parquet_files if 'training_data' in f]
    
    if historical_files:
        latest_file = max(historical_files, key=lambda x: x.split('_')[-1] if '_' in x else x)
        print(f"\nDownloading historical data (original method): {latest_file}")
    elif training_files:
        latest_file = max(training_files, key=lambda x: x.split('_')[-1] if '_' in x else x)
        print(f"\nDownloading training data: {latest_file}")
    else:
        latest_file = max(parquet_files, key=lambda x: x.split('_')[-1] if '_' in x else x)
        print(f"\nDownloading: {latest_file}")
    
    df = download_parquet_from_swift(latest_file)
    
    if df is not None:
        print(f"Downloaded {len(df)} records")
        print(df.head())
        
        
        # Prepare data for Prophet (needs 'ds' and 'y' columns)
        if 'time' in df.columns:
            df['ds'] = pd.to_datetime(df['time'])
        else:
            print("Warning: No 'time' column found. Please check your data structure.")
        
        # Select the value column for 'y' (adjust based on your data)
        value_columns = [col for col in df.columns if col not in ['ds', 'time', 'sensor_id']]
        if value_columns:
            print(f"Available value columns: {value_columns}")
            
            # Train separate models for each sensor and each metric
            sensors = df['sensor_id'].unique()
            print(f"Training models for sensors: {sensors}")
            
            for sensor in sensors:
                sensor_data = df[df['sensor_id'] == sensor].copy()
                print(f"\nTraining model for {sensor} with {len(sensor_data)} records")
                
                for metric in value_columns:
                    print(f"  Training {metric} model...")
                    
                    # Prepare data for this sensor and metric
                    train_df = sensor_data[['ds', metric]].copy()
                    train_df.columns = ['ds', 'y']
                    train_df = train_df.dropna()
                    
                    if len(train_df) < 10:  # Need minimum data points
                        print(f"    Skipping {metric} - insufficient data ({len(train_df)} points)")
                        continue
                    
                    # Train Prophet model
                    model = Prophet(
                        daily_seasonality=True,
                        weekly_seasonality=True,
                        yearly_seasonality=False
                    )
                    model.fit(train_df)
                    
                    # Make future predictions (7 days ahead)
                    future = model.make_future_dataframe(periods=7*24*12)  # 7 days, 5-min intervals
                    forecast = model.predict(future)
                    
                    # Plot results
                    fig = model.plot(forecast)
                    plt.title(f'{sensor} - {metric} Forecast')
                    plt.savefig(f'prophet_{sensor}_{metric}_forecast.png')
                    print(f"    Forecast plot saved as 'prophet_{sensor}_{metric}_forecast.png'")
                    
                    # Also show forecast components
                    fig2 = model.plot_components(forecast)
                    plt.savefig(f'prophet_{sensor}_{metric}_components.png')
                    print(f"    Components plot saved as 'prophet_{sensor}_{metric}_components.png'")
                    
                    # Print forecast summary
                    print(f"    Forecast Summary for {metric}:")
                    print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(3))
                    
                    plt.close('all')  # Close plots to free memory
        else:
            print("No value columns found for training")
    else:
        print("Failed to download data")
else:
    print("No parquet files found in storage")