from datetime import datetime
import math
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Union
import sys
sys.path.append("./flows")
from config import get_mongo_db


# Modèles
class CAParPays(BaseModel):
    pays: str
    ca_total: float
    nb_achats: int
    panier_moyen: float


class VolumesJour(BaseModel):
    jour: str
    nb_achats: int
    ca_total: float


class VolumesMois(BaseModel):
    mois: str
    nb_achats: int
    ca_total: float


class Croissance(BaseModel):
    mois: str
    nb_achats: int
    ca_total: float
    ca_mois_precedent: Optional[float] = None
    croissance_pct: Optional[float] = None



class Distribution(BaseModel):
    nb_achats: int
    montant_moyen: float
    montant_median: float
    montant_min: float
    montant_max: float
    ecart_type: float



app = FastAPI(
    title="Data Lake API",
    description="API pour accéder aux KPIs du Data Lake",
    version="1.0.0"
)



def clean_data(data: list) -> list:
    """
        Clean data for JSON serialization
        Remove datetime objects and Nan/Inf values
    """
    for doc in data:
        for key, value in doc.items():
            # Convertir datetime en string
            if isinstance(value, datetime):
                doc[key] = value.strftime("%Y-%m-%d")
            # Convertir NaN/Inf en None
            elif isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                doc[key] = None
    return data



@app.get("/", tags=["Home"])
def read_root():
    """
    Home
    """
    return {
        "message": "Bienvenue sur l'API Data Lake",
        "documentation": "/docs",
        "endpoints": [
            "/api/ca_par_pays",
            "/api/volumes_jour",
            "/api/volumes_mois",
            "/api/croissance",
            "/api/distribution"
        ]
    }


@app.get("/api/ca_par_pays", response_model=list[CAParPays], tags=["KPIs"])
def get_ca_par_pays():
    """
        Get the revenue by country
    """
    db = get_mongo_db()
    data = list(db["kpi_ca_par_pays"].find({}, {"_id": 0}))
    
    if not data:
        raise HTTPException(status_code=404, detail="Aucune donnée trouvée")
    
    return clean_data(data)


@app.get("/api/volumes_jour", response_model=list[VolumesJour], tags=["KPIs"])
def get_volumes_jour():
    """ 
        Get the purchase volumes by day
    """
    db = get_mongo_db()
    data = list(db["kpi_volumes_jour"].find({}, {"_id": 0}))
    
    if not data:
        raise HTTPException(status_code=404, detail="Aucune donnée trouvée")
    
    return clean_data(data)


@app.get("/api/volumes_mois", response_model=list[VolumesMois], tags=["KPIs"])
def get_volumes_mois():
    """
        Get the purchase volumes by month
    """
    db = get_mongo_db()
    data = list(db["kpi_volumes_mois"].find({}, {"_id": 0}))
    
    if not data:
        raise HTTPException(status_code=404, detail="Aucune donnée trouvée")
    
    return clean_data(data)


@app.get("/api/croissance", response_model=list[Croissance], tags=["KPIs"])
def get_croissance():
    """
        Get the growth KPIs
    """
    db = get_mongo_db()
    data = list(db["kpi_croissance"].find({}, {"_id": 0}))
    
    if not data:
        raise HTTPException(status_code=404, detail="Aucune donnée trouvée")
    
    return clean_data(data)


@app.get("/api/distribution", response_model=list[Distribution], tags=["KPIs"])
def get_distribution():
    """
        Get the distribution KPIs
    """
    db = get_mongo_db()
    data = list(db["kpi_distribution"].find({}, {"_id": 0}))
    
    if not data:
        raise HTTPException(status_code=404, detail="Aucune donnée trouvée")
    
    return clean_data(data)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)