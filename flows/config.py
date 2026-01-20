import os
from pathlib import Path

from dotenv import load_dotenv
from minio import Minio

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

load_dotenv()

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "False").lower() == "true"

# MongoDB configuration
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "datalake")

# Database configuration
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "./data/database/analytics.db")

# Prefect configuration
PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")

# Buckets
BUCKET_SOURCES = "sources"
BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"
BUCKET_GOLD = "gold"

def get_minio_client() -> Minio:
    """Initialize and return a MinIO client."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )

def get_mongo_client() -> MongoClient:
    """Connexion à MongoDB Atlas."""
    return MongoClient(MONGO_URI, server_api=ServerApi('1'))

def get_mongo_db():
    """Récupérer la base de données MongoDB."""
    client = get_mongo_client()
    return client[MONGO_DB]

def configure_prefect()-> None:
    """Configure Prefect settings."""
    os.environ["PREFECT_API_URL"] = PREFECT_API_URL

if __name__ == "__main__":
    client = get_minio_client()
    print(client.list_buckets())  
    configure_prefect()
    print(os.getenv("PREFECT_API_URL"))

    try:
        mongo_client = get_mongo_client()
        mongo_client.admin.command('ping')
        print("MongoDB connected successfully.")
    except Exception as e:
        print(f"MongoDB error: {e}")
    