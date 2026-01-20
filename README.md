# Data Lake - TP BigData

Un pipeline de données complet qui ingère, nettoie et expose des données via une API REST et un dashboard interactif.

## Ce que ça fait

- **Bronze** : Données brutes en CSV
- **Silver** : Données nettoyées et transformées en Parquet
- **Gold** : Agrégations et KPIs prêts pour l'analyse
- **MongoDB** : Stockage des KPIs pour accès rapide
- **FastAPI** : API REST pour requêter les données
- **Streamlit** : Dashboard interactif

## Architecture

```
CSV (sources) → Bronze (MinIO)
    ↓
Nettoyage → Silver (MinIO)
    ↓
Agrégation → Gold (MinIO)
    ↓
Export → MongoDB
    ↓
API (FastAPI) ← Dashboard (Streamlit)
```

## Installation

### Prérequis

- Python 3.10+
- Docker (pour MinIO, MongoDB, etc.)
- pip

### 1. Clone et environnement

```bash
cd /Users/heloiselelez/Documents/Cours_M2/BigData/TP
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configuration

Créez un fichier `.env` :

```
MINIO_ENPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_SECURE=False

MONGO_URI=mongodb+srv://user:password@cluster.mongodb.net/datalake?appName=Cluster
MONGO_DB=datalake

PREFECT_API_URL=http://localhost:4200/api
```

### 3. Lancez les services

```bash
# MinIO (data lake)
docker run -d -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  --name minio minio/minio server /data --console-address ":9001"

# MongoDB (optionnel si vous utilisez Atlas)
docker run -d -p 27017:27017 --name mongodb mongo:latest

# Prefect (orchestration)
prefect server start
```

## Lancer le pipeline

### Générer les données de test

```bash
python script/generate_data.py
```

### Exécuter les flows

```bash
# Bronze layer
python -m flows.bronze_ingestion

# Silver layer
python -m flows.silver_ingestion

# Gold layer
python -m flows.gold_ingestion

# Export vers MongoDB
python flows/mongodb_ingestion.py
```

Ou utilisez Prefect :

```bash
prefect flow run -n bronze_ingestion_flow
prefect flow run -n silver_ingestion_flow
prefect flow run -n gold_ingestion_flow
```

## Lancer l'API

```bash
python api/main.py
```

L'API tourne sur `http://localhost:5000`

Swagger : `http://localhost:5000/docs`

Endpoints :
- `/api/ca_par_pays` - CA by country
- `/api/volumes_jour` - Daily volumes
- `/api/volumes_mois` - Monthly volumes
- `/api/croissance` - Growth rate
- `/api/distribution` - Statistical distribution

## Lancer le dashboard

```bash
streamlit run dashboard/app.py
```

Accédez à `http://localhost:8501`

Onglets disponibles :
- **Accueil** : Comparaison temps MongoDB vs MinIO
- **CA par Pays** : Visualisation par pays
- **Volumes** : Jour/Mois/Année
- **Croissance** : Évolution du taux de croissance
- **Distribution** : Statistiques
- **MinIO Data** : Explorer les données brutes du bucket Gold

## Structure du projet

```
├── flows/               # Pipeline d'ingestion
│   ├── config.py       # Config MinIO/MongoDB
│   ├── bronze_ingestion.py
│   ├── silver_ingestion.py
│   ├── gold_ingestion.py
│   └── mongodb_ingestion.py
├── api/
│   └── main.py         # FastAPI server
├── dashboard/          # Streamlit dashboard
│   ├── app.py
│   ├── utils.py
│   └── tabs/           # Onglets individuels
├── script/
│   └── generate_data.py
├── data/sources/       # Données CSV d'entrée
└── requirements.txt
```

## Développement

### Ajouter une nouvelle métrique

1. Créez une fonction dans `flows/gold_ingestion.py`
2. Exportez-la dans `flows/mongodb_ingestion.py`
3. Ajoutez un endpoint dans `api/main.py`
4. Créez un tab dans `dashboard/tabs/`

### Vérifier les données

```bash
# Vérifier Silver
python script/verify_silver.py

# Vérifier Gold
python script/verify_gold.py
```

## Notes

- Les données de test sont générées aléatoirement
- MongoDB Atlas est utilisé en production
- MinIO local pour développement
- Streamlit recharge automatiquement à chaque modification
