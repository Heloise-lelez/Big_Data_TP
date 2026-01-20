from io import BytesIO
import pandas as pd

from prefect import flow, task

from config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client


@task(name="read_from_bronze", retries=2)
def read_csv_from_bronze(object_name: str) -> pd.DataFrame:
    """
    Read CSV file from Bronze bucket into a Pandas DataFrame.
    Args:
        object_name: Name of the object in Bronze bucket

    Returns:
        pd.DataFrame: DataFrame containing the CSV data
    """
    client = get_minio_client()

    response = client.get_object(BUCKET_BRONZE, object_name)
    data = response.read()

    response.close()
    response.release_conn()

    df = pd.read_csv(BytesIO(data))
    return df


@task(name="clean_dataframe")
def clean_dataframe(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """
    Apply Silver transformations:
    - Handle missing values
    - Normalize data types
    - Standardize dates
    - Deduplicate records
    """
    df = df.copy()

    # Supprimer les lignes entièrement vides
    df = df.dropna(how="all")

    # supprimer les doubles id
    if "id_client" in df.columns and dataset_name == "clients":
        df = df.drop_duplicates(subset=["id_client"])

    if "id_achat" in df.columns and dataset_name == "achats":
        df = df.drop_duplicates(subset=["id_achat"])

    # Convertion des dates
    for col in df.columns:
        if "date" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)
        
            null_count = df[col].isna().sum()
            if null_count > 0:
                print(f"{col}: {null_count} valeurs non converties")

    # Nettoyer les strings
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    # Supprimer les doublons
    df = df.drop_duplicates()

    print(f"{dataset_name}: {len(df)} rows after cleaning")
    return df

@task(name="data_quality_checks")
def data_quality_checks(df: pd.DataFrame, dataset_name: str) -> None:
    """
    Perform data quality checks on the DataFrame.
    Args:
        df: DataFrame to check
        dataset_name: Name of the dataset 
    
    returns: 
    """
    if df.empty:
        raise ValueError(f"[Data Quality] {dataset_name} DataFrame is empty!")

    if df.isnull().all().any():
        raise ValueError(f"[Data Quality] {dataset_name} DataFrame has columns with all null values!")


@task(name="write_to_silver", retries=2)
def write_df_to_silver(df: pd.DataFrame, object_name: str) -> str:
    """
    Write DataFrame to Silver bucket in Parquet format.
    """
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)

    data = df.to_parquet(index=False)

    client.put_object(
        BUCKET_SILVER,
        object_name,
        BytesIO(data),
        length=len(data)
    )

    return object_name

@flow(name="Silver Transformation Flow")
def silver_transformation_flow():
    # Clients
    clients_df = read_csv_from_bronze("clients.csv")
    clients_clean = clean_dataframe(clients_df, "clients")
    data_quality_checks(clients_clean, "clients")
    silver_clients = write_df_to_silver(clients_clean, "clients.parquet")

    # Achats
    achats_df = read_csv_from_bronze("achats.csv")
    achats_clean = clean_dataframe(achats_df, "achats")
    data_quality_checks(achats_clean, "achats")
    silver_achats = write_df_to_silver(achats_clean, "achats.parquet")

    return {
        "clients": silver_clients,
        "achats": silver_achats
    }
if __name__ == "__main__":
    result = silver_transformation_flow()
    print(f"Silver ingestion complete: {result}")
