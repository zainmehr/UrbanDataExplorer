import pandas as pd
from fastapi import APIRouter, HTTPException, Query
import numpy as np
import json
import utils 

# Création du routeur API
router = APIRouter(prefix="/api")

# --- Endpoints Géographiques ---

@router.get("/geojson", tags=["Géographie"])
async def get_geojson():
    """ Renvoie le GeoJSON des arrondissements pour dessiner la carte. """
    if not utils.geojson_data:
        raise HTTPException(status_code=500, detail="Fichier GeoJSON non chargé.")
    return utils.geojson_data

@router.get("/arrondissements", tags=["Arrondissements"])
async def get_arrondissements_list():
    if utils.df_gold.empty:
        raise HTTPException(status_code=503, detail="Données non disponibles.")
    
    arr_list = utils.df_gold['Arrondissement'].sort_values().unique().tolist()
    
    return {
        "arrondissements": arr_list, 
        "annee_max": int(utils.DEFAULT_YEAR),  
        "annee_min": int(utils.START_YEAR)     
    }


# --- Endpoints de Filtrage et de Données ---

@router.get("/prix", tags=["Filtres & Carte"])
async def get_prix_by_year(annee: int = Query(utils.DEFAULT_YEAR, description="Année de mutation désirée (2020-2025).")):
    """ 
    Endpoint principal pour la carte (Exigence : /prix?annee=2023). 
    """
    if utils.df_gold.empty:
        raise HTTPException(status_code=503, detail="Données non disponibles.")
    
    if annee not in range(utils.START_YEAR, utils.DEFAULT_YEAR + 1):
        raise HTTPException(status_code=400, detail=f"Année non valide. Choisissez entre {utils.START_YEAR} et {utils.DEFAULT_YEAR}.")

    df_filtered = utils.df_gold[utils.df_gold['annee_mutation'] == annee].copy()

    if df_filtered.empty:
        raise HTTPException(status_code=404, detail=f"Aucune donnée trouvée pour l'année {annee}.")
    
    cols_to_return = [
        'Arrondissement', 'annee_mutation',
        'prix_m2_median', 'var_an_pct', 'part_logmt_sociaux_pct',
        'part_rp_1p_pct', 'part_rp_2p_pct', 'part_rp_3p_pct', 'part_rp_4p_et_plus_pct', 
        'part_maisons_pct'
    ]
    
    return df_filtered[cols_to_return].to_dict(orient="records")




@router.get("/timeline", tags=["Filtres & Carte"])
async def get_timeline_data(arr: int = Query(..., description="Numéro de l'arrondissement (Exigence : /timeline?arr=6).")):
    if utils.df_gold.empty:
        raise HTTPException(status_code=503, detail="Données non disponibles.")
        
    if arr not in utils.df_gold['Arrondissement'].unique():
        raise HTTPException(status_code=404, detail=f"Arrondissement {arr} non trouvé.")
        
    df_filtered = utils.df_gold[utils.df_gold['Arrondissement'] == arr].copy()
    
    cols_to_return = ['annee_mutation', 'prix_m2_median', 'var_an_pct']
    
    df_filtered = df_filtered[cols_to_return]
    
    df_filtered = df_filtered.replace([np.inf, -np.inf], np.nan) 
    
    
    # Conversion du DataFrame en JSON string
    json_string = df_filtered.to_json(orient='records', double_precision=2, date_format='iso')
    
    # Charger la chaîne JSON en liste de dictionnaires Python
    timeline_data = json.loads(json_string) 
    
    return {
        "arrondissement": arr,
        "timeline": timeline_data
    }
    



@router.get("/comparaison", tags=["Comparaison"])
async def get_comparison_data(
    arr1: int = Query(..., description="Premier arrondissement (Exigence : /comparaison?arr1=1&arr2=6)"),
    arr2: int = Query(..., description="Deuxième arrondissement")
):
    if utils.df_gold.empty:
        raise HTTPException(status_code=503, detail="Données non disponibles.")

    valid_arr = utils.df_gold['Arrondissement'].unique()
    
    if arr1 not in valid_arr or arr2 not in valid_arr:
        raise HTTPException(status_code=400, detail="Un ou les deux arrondissements ne sont pas valides.")
        
    df_filtered = utils.df_gold[utils.df_gold['Arrondissement'].isin([arr1, arr2])].copy()
    
    
    # 1. Remplacer les valeurs infinies (inf) par NaN dans le DataFrame filtré
    df_filtered = df_filtered.replace([np.inf, -np.inf], np.nan) 

    # 2. Utiliser to_json() puis json.loads() pour garantir la conversion de NaN en 'null'
    # On renvoie la table complète pour la comparaison
    
    # Conversion du DataFrame en JSON string
    json_string = df_filtered.to_json(orient='records', double_precision=2, date_format='iso')
    
    # Charger la chaîne JSON en liste de dictionnaires Python
    comparison_data = json.loads(json_string) 

    return comparison_data # On renvoie directement la liste de dictionnaires

