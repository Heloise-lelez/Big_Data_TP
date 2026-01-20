import streamlit as st
import plotly.express as px
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import fetch_data, get_minio_data


def show():
    st.header("Croissance")
    df_croissance, time_mongo = fetch_data("/api/croissance")
    _, time_minio = get_minio_data("gold", "kpi_croissance")
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("MongoDB", f"{time_mongo:.0f}ms")
    with col2:
        st.metric("MinIO", f"{time_minio:.0f}ms")
    
    st.divider()
    
    if not df_croissance.empty:

        if 'mois' in df_croissance.columns:
            df_croissance['mois'] = pd.to_datetime(df_croissance['mois'], errors='coerce')
        
    

            fig = px.line(df_croissance, x="mois", y="croissance_pct",
                         title="Taux de Croissance")
            st.plotly_chart(fig, use_container_width=True)
        
    
        
        st.dataframe(df_croissance, use_container_width=True)
