import streamlit as st
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import get_minio_data


def show():
    """Affiche l'onglet MinIO Data."""
    st.header("Exploration des données MinIO")
    

    selected_bucket = "gold"
    df = get_minio_data(selected_bucket, "")
    
    if not df.empty:

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Nombre de lignes", len(df))
        with col2:
            st.metric("Nombre de colonnes", len(df.columns))
        with col3:
            st.metric("Taille approximative", f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        

        st.subheader("Aperçu des données")
        st.dataframe(df, use_container_width=True)
        

        st.subheader("Filtrage des données")
        columns = df.columns.tolist()
        selected_column = st.selectbox("Sélectionnez une colonne pour filtrer:", columns)
        
        if selected_column:
            unique_values = df[selected_column].unique()
            if len(unique_values) <= 100:
                selected_value = st.multiselect(f"Valeurs de {selected_column}:", unique_values)
                if selected_value:
                    df_filtered = df[df[selected_column].isin(selected_value)]
                    st.dataframe(df_filtered, use_container_width=True)
        

        csv = df.to_csv(index=False)
        st.download_button(
            label="Télécharger en CSV",
            data=csv,
            file_name=f"{selected_bucket}_data.csv",
            mime="text/csv"
        )
    else:
        st.warning("Aucune donnée trouvée dans le bucket")
