import streamlit as st
import plotly.express as px
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import fetch_data


def show():
    """Affiche l'onglet Volumes avec sous-onglets."""
    st.header("Volumes")
    
    sub_tabs = st.tabs(["Par Jour", "Par Mois", "Par An"])
    
    with sub_tabs[0]:
        st.subheader("Volumes par Jour")
        df_jour = fetch_data("/api/volumes_jour")
        
        if not df_jour.empty:
            if 'jour' in df_jour.columns:
                df_jour['jour'] = pd.to_datetime(df_jour['jour'], errors='coerce')
            
            fig = px.line(df_jour, x="jour", y="nb_achats", 
                         title="Nombre d'achats par jour")
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df_jour, use_container_width=True)
    
    with sub_tabs[1]:
        st.subheader("Volumes par Mois")
        df_mois = fetch_data("/api/volumes_mois")
        
        if not df_mois.empty:

            if 'mois' in df_mois.columns:
                df_mois['mois'] = pd.to_datetime(df_mois['mois'], errors='coerce')
            
            fig = px.line(df_mois, x="mois", y="nb_achats",
                        title="Nombre d'achats par mois")
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df_mois, use_container_width=True)
    
    with sub_tabs[2]:
        st.subheader("Volumes par an")
        df_jour = fetch_data("/api/volumes_jour")
        
        if not df_jour.empty:
            if 'jour' in df_jour.columns:
                df_jour['jour'] = pd.to_datetime(df_jour['jour'], errors='coerce')
                df_annuel = df_jour.groupby(df_jour['jour'].dt.year)['nb_achats'].sum().reset_index()
                df_annuel.columns = ['annee', 'nb_achats']
                
                fig = px.line(df_annuel, x="annee", y="nb_achats",
                           title="Nombre d'achats par année")
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(df_annuel, use_container_width=True)
