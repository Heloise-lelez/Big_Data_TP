from io import BytesIO
import pandas as pd

from prefect import flow, task

from config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client


@task(name="read_from_silver")
def read_parquet_from_silver(object_name: str) -> pd.DataFrame:
    """Lire un fichier Parquet depuis le bucket Silver."""
    client = get_minio_client()

    response = client.get_object(BUCKET_SILVER, object_name)
    df = pd.read_parquet(BytesIO(response.read()))
    response.close()
    response.release_conn()
    return df


@task(name="create_dim_clients")
def create_dim_clients(clients_df: pd.DataFrame) -> pd.DataFrame:
    """Créer la table dimension clients."""
    dim = clients_df.copy()
    
    dim["annee_inscription"] = dim["date_inscription"].dt.year

    return dim


@task(name="create_dim_temps")
def create_dim_temps(achats_df: pd.DataFrame) -> pd.DataFrame:
    """Créer la table dimension temps."""

    dates = achats_df["date_achat"].dropna().unique()
    
    dim = pd.DataFrame({"date": pd.to_datetime(dates)})
    dim = dim.drop_duplicates().sort_values("date")
    

    dim["annee"] = dim["date"].dt.year
    dim["mois"] = dim["date"].dt.month
    dim["jour"] = dim["date"].dt.day
    dim["semaine"] = dim["date"].dt.isocalendar().week
    dim["jour_semaine"] = dim["date"].dt.day_name()

    return dim


@task(name="create_fact_achats")
def create_fact_achats(achats_df: pd.DataFrame, clients_df: pd.DataFrame) -> pd.DataFrame:
    """Créer la table de faits en joignant achats et clients."""
    fact = achats_df.merge(
        clients_df[["id_client", "pays"]], 
        on="id_client", 
        how="left"
    )

    return fact



@task(name="kpi_volumes_par_periode")
def kpi_volumes_par_periode(fact_achats: pd.DataFrame) -> dict:
    """KPI: Volumes et CA par jour, semaine, mois."""
    df = fact_achats.copy()
    
    # Par jours
    df["jour"] = df["date_achat"].dt.date
    volumes_jour = df.groupby("jour").agg(
        nb_achats=("id_achat", "count"),
        ca_total=("montant", "sum")
    ).reset_index()
    
    # Par semaine
    df["semaine"] = df["date_achat"].dt.to_period("W").astype(str)
    volumes_semaine = df.groupby("semaine").agg(
        nb_achats=("id_achat", "count"),
        ca_total=("montant", "sum")
    ).reset_index()
    
    # Par mois
    df["mois"] = df["date_achat"].dt.to_period("M").astype(str)
    volumes_mois = df.groupby("mois").agg(
        nb_achats=("id_achat", "count"),
        ca_total=("montant", "sum")
    ).reset_index()
    
    
    return {
        "jour": volumes_jour,
        "semaine": volumes_semaine,
        "mois": volumes_mois
    }


@task(name="kpi_ca_par_pays")
def kpi_ca_par_pays(fact_achats: pd.DataFrame) -> pd.DataFrame:
    """KPI: Chiffre d'affaires par pays."""
    kpi = fact_achats.groupby("pays").agg(
        nb_achats=("id_achat", "count"),
        ca_total=("montant", "sum"),
        panier_moyen=("montant", "mean")
    ).reset_index()
    
    kpi["panier_moyen"] = kpi["panier_moyen"].round(2)

    return kpi


@task(name="kpi_croissance")
def kpi_croissance(volumes_mois: pd.DataFrame) -> pd.DataFrame:
    """KPI: Taux de croissance mensuel."""
    df = volumes_mois.copy()
    df = df.sort_values("mois")
    
    df["ca_mois_precedent"] = df["ca_total"].shift(1)
    df["croissance_pct"] = ((df["ca_total"] - df["ca_mois_precedent"]) / df["ca_mois_precedent"] * 100).round(2)

    return df


@task(name="kpi_distribution")
def kpi_distribution(fact_achats: pd.DataFrame) -> pd.DataFrame:
    """KPI: Distribution statistique des montants."""
    montants = fact_achats["montant"]
    
    stats = pd.DataFrame([{
        "nb_achats": len(montants),
        "montant_moyen": round(montants.mean(), 2),
        "montant_median": round(montants.median(), 2),
        "montant_min": round(montants.min(), 2),
        "montant_max": round(montants.max(), 2),
        "ecart_type": round(montants.std(), 2)
    }])
    
    return stats


@task(name="write_to_gold")
def write_to_gold(df: pd.DataFrame, object_name: str) -> str:
    """Écrire un DataFrame dans le bucket Gold."""
    client = get_minio_client()


    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)

    data = df.to_parquet(index=False)


    client.put_object(
        BUCKET_GOLD,
        object_name,
        BytesIO(data),
        length=len(data)
    )

    return object_name



@flow(name="Gold Transformation Flow")
def gold_transformation_flow():
    """
    Flow Gold : créer les dimensions, faits et KPIs.
    """

    clients_df = read_parquet_from_silver("clients.parquet")
    achats_df = read_parquet_from_silver("achats.parquet")
    

    dim_clients = create_dim_clients(clients_df)
    dim_temps = create_dim_temps(achats_df)
    

    fact_achats = create_fact_achats(achats_df, clients_df)
    

    volumes = kpi_volumes_par_periode(fact_achats)
    ca_pays = kpi_ca_par_pays(fact_achats)
    croissance = kpi_croissance(volumes["mois"])
    distribution = kpi_distribution(fact_achats)
    

    results = {}
    

    results["dim_clients"] = write_to_gold(dim_clients, "dim_clients.parquet")
    results["dim_temps"] = write_to_gold(dim_temps, "dim_temps.parquet")
    

    results["fact_achats"] = write_to_gold(fact_achats, "fact_achats.parquet")
    

    results["volumes_jour"] = write_to_gold(volumes["jour"], "kpi_volumes_jour.parquet")
    results["volumes_semaine"] = write_to_gold(volumes["semaine"], "kpi_volumes_semaine.parquet")
    results["volumes_mois"] = write_to_gold(volumes["mois"], "kpi_volumes_mois.parquet")
    results["ca_pays"] = write_to_gold(ca_pays, "kpi_ca_par_pays.parquet")
    results["croissance"] = write_to_gold(croissance, "kpi_croissance.parquet")
    results["distribution"] = write_to_gold(distribution, "kpi_distribution.parquet")
    
    return results


if __name__ == "__main__":
    result = gold_transformation_flow()
    print(f"Gold ingestion complete: {result}")
    for nom, fichier in result.items():
        print(f"{nom}: {fichier}")
