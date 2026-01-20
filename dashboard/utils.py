import streamlit as st
import pandas as pd
import requests
from io import BytesIO
import sys
from pathlib import Path
import time

sys.path.append("./flows")
from config import get_minio_client

API_URL = "http://localhost:5000"


def fetch_data(endpoint: str) -> pd.DataFrame:
    try:
        response = requests.get(f"{API_URL}{endpoint}")
        if response.status_code == 200:
            return pd.DataFrame(response.json())
    except requests.exceptions.ConnectionError:
        st.error("API not running. Please start the FastAPI server.")
    
    return pd.DataFrame()


def get_minio_data(bucket: str, prefix: str) -> pd.DataFrame:
    try:
        client = get_minio_client()
        objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
        
        dataframes = []
        for obj in objects:
            if obj.object_name.endswith(('.parquet', '.csv', '.json')):
                try:
                    data = client.get_object(bucket, obj.object_name)
                    
                    if obj.object_name.endswith('.parquet'):
                        df = pd.read_parquet(BytesIO(data.read()))
                    elif obj.object_name.endswith('.csv'):
                        df = pd.read_csv(BytesIO(data.read()))
                    elif obj.object_name.endswith('.json'):
                        df = pd.read_json(BytesIO(data.read()))
                    
                    dataframes.append(df)
                    
                except Exception as e:
                    pass
        
        if dataframes:
            return pd.concat(dataframes, ignore_index=True)
        
        return pd.DataFrame()
        
    except Exception as e:
        return pd.DataFrame()


def measure_api_time() -> float:
    start = time.time()
    fetch_data("/api/ca_par_pays")
    return (time.time() - start) * 1000


def measure_minio_time() -> float:
    start = time.time()
    get_minio_data("gold", "")
    return (time.time() - start) * 1000
