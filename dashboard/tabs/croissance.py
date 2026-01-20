import streamlit as st
import plotly.express as px
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import fetch_data


def show():
    st.header("Croissance")
    df_croissance = fetch_data("/api/croissance")
    
    if not df_croissance.empty:

        if 'mois' in df_croissance.columns:
            df_croissance['mois'] = pd.to_datetime(df_croissance['mois'], errors='coerce')
        
    

            fig = px.line(df_croissance, x="mois", y="croissance_pct",
                         title="Taux de Croissance")
            st.plotly_chart(fig, use_container_width=True)
        
    
        
        st.dataframe(df_croissance, use_container_width=True)
