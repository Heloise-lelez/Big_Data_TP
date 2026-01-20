import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "tabs"))

from tabs import home, ca_par_pays, volumes, croissance, distribution, minio_data

st.set_page_config(
    page_title="Dashboard KPIs",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Dashboard d'Analyse KPIs")

tabs = st.tabs([
    "Accueil",
    "CA par Pays",
    "Volumes",
    "Croissance",
    "Distribution",
])

with tabs[0]:
    home.show()

with tabs[1]:
    ca_par_pays.show()

with tabs[2]:
    volumes.show()

with tabs[3]:
    croissance.show()

with tabs[4]:
    distribution.show()
