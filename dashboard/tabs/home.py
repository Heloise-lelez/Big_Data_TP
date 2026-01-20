import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import fetch_data, get_minio_data


def measure_api_time() -> float:
    _, time_taken = fetch_data("/api/ca_par_pays")
    return time_taken


def measure_minio_time() -> float:
    _, time_taken = get_minio_data("gold", "")
    return time_taken
    return time_taken


def show():
    st.header("Accueil")
    
    if st.button("Calculer les temps"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("MongoDB")
            api_time = measure_api_time()
            st.metric("Temps de refresh", f"{api_time:.2f}ms")
        
        with col2:
            st.subheader("MinIO")
            minio_time = measure_minio_time()
            st.metric("Temps de refresh", f"{minio_time:.2f}ms")
