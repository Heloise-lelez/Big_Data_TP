import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import measure_api_time, measure_minio_time


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
