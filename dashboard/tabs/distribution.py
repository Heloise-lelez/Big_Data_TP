import streamlit as st
import plotly.express as px
import pandas as pd
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import fetch_data


def show():
    st.header("Distributions Statistiques")
    df_dist = fetch_data("/api/distribution")
    
    if not df_dist.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Statistiques du Montant")
            st.metric("Montant Moyen", f"{df_dist['montant_moyen'].iloc[0]:.2f}€")
            st.metric("Montant Médian", f"{df_dist['montant_median'].iloc[0]:.2f}€")
            st.metric("Écart-type", f"{df_dist['ecart_type'].iloc[0]:.2f}€")
        
        with col2:
            st.subheader("Statistiques Générales")
            st.metric("Nombre d'Achats", f"{int(df_dist['nb_achats'].iloc[0])}")
            st.metric("Montant Min", f"{df_dist['montant_min'].iloc[0]:.2f}€")
            st.metric("Montant Max", f"{df_dist['montant_max'].iloc[0]:.2f}€")
        
        st.dataframe(df_dist, use_container_width=True)
