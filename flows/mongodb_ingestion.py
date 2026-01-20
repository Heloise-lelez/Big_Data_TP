from datetime import date, datetime
from io import BytesIO
import pandas as pd

from prefect import flow, task

from config import get_minio_client, get_mongo_db, BUCKET_GOLD



@task(name="read_from_gold")
def read_from_gold(object_name:str)-> pd.DataFrame:
    """
    Read a Parquet file from the Gold bucket.

    Args:
        object_name: Name of the object in MinIO

    Returns:
        pd.DataFrame: DataFrame containing the Parquet data
    """

    client = get_minio_client()
    response = client.get_object(BUCKET_GOLD, object_name)
    df = pd.read_parquet(BytesIO(response.read()))
    print(f"Chargé {object_name}, {len(df)} lignes")
    return df

@task(name="export_to_mongodb")
def export_to_mongodb(df:pd.DataFrame, collection_name:str)-> int:
    """
    Export data from Gold layer to MongoDB.

    Args:
        df: DataFrame to export
        collection_name: Name of the MongoDB collection

    Returns:
        int: Number of documents inserted
    """
    db=get_mongo_db()

    df = df.copy()
    for col in df.columns:
        # convertit les dates en datetime
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: datetime.combine(x, datetime.min.time()) if isinstance(x, date) else x)
        # convertit les datetime64 en datetime pyhton
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.to_pydatetime()



    records= df.to_dict(orient="records")

    # deelte l'ancienne collection
    db[collection_name].drop()

    if records:
        db[collection_name].insert_many(records)
        print(f"Exported {len(records)} documents to '{collection_name}'")
    return len(records)

@flow(name="MongoDB Ingestion Flow")
def mongodb_ingestion_flow():
    """
    Main flow to ingest data from Gold layer to MongoDB.
    """
    fichiers = {
        "kpi_volumes_jour.parquet": "kpi_volumes_jour",
        "kpi_volumes_semaine.parquet": "kpi_volumes_semaine",
        "kpi_volumes_mois.parquet": "kpi_volumes_mois",
        "kpi_ca_par_pays.parquet": "kpi_ca_par_pays",
        "kpi_croissance.parquet": "kpi_croissance",
        "kpi_distribution.parquet": "kpi_distribution"
    }
    
    results = {}
    
    for fichier, collection in fichiers.items():
        df = read_from_gold(fichier)
        count = export_to_mongodb(df, collection)
        results[collection] = count
    
    return results

if __name__ == "__main__":
    result = mongodb_ingestion_flow()

    print(f"MongoDB ingestion complete: {result}")

    for collection, count in result.items():
        print(f"{collection}: {count} documents")