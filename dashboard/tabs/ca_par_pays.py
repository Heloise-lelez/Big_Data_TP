import streamlit as st
import plotly.express as px
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import fetch_data, get_minio_data


def show():
    st.header("Chiffre d'Affaires par Pays")
    df_ca, time_mongo = fetch_data("/api/ca_par_pays")
    _, time_minio = get_minio_data("gold", "kpi_ca_par_pays")
    
    if not df_ca.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.metric("MongoDB", f"{time_mongo:.0f}ms")
        with col2:
            st.metric("MinIO", f"{time_minio:.0f}ms")
        
        st.divider()
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(df_ca, x="pays", y="ca_total", title="CA Total par Pays")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.pie(df_ca, values="ca_total", names="pays", title="Répartition du CA")
            st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(df_ca, use_container_width=True)
