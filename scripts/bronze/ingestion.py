import os
import requests
import pandas as pd
import json

# Ce code est à exécuter depuis le répertoire scripts/bronze

chemin_bronze_relatif = "../../data/bronze"
chemin_bronze = os.path.abspath(chemin_bronze_relatif)

Data_sources = {
    # "dvf": {
    #     "url" : "https://static.data.gouv.fr/resources/demandes-de-valeurs-foncieres-geolocalisees/20251024-114956/dvf.csv.gz",
    #     "format":"csv",
    #     "filename":"valeurs_foncieres.csv.gz"
    # },
    # "logement_sociaux" : {
    #     "url" : "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/logements-sociaux-finances-a-paris/exports/csv?use_labels=true",
    #     "format":"csv",
    #     "filename":"logements_sociaux.csv"
    # },
    "insee_logement" : {
        "url" : "https://www.insee.fr/fr/statistiques/fichier/8202349/base-cc-logement-2021_csv.zip",
        "format": "zip", 
        "filename": "recensement_logement.zip" 
    },
    "qualite_air" : {
        "url" : "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/qualite-de-l-air-exposition-des-parisen-ne-s-au-no2-et-pm2-5/exports/csv?use_labels=true",
        "format": "csv",
        "filename": "qualite_air_citeair.csv"
    }
}

def collect_data_to_bronze(source_key, source_info) :
    url = source_info["url"]
    filename = source_info["filename"]

    chemin_destination = os.path.join(chemin_bronze,filename)

    reponse = requests.get(url)
    if reponse.status_code == 200 :
        try :
            with open(chemin_destination, "wb") as f :
                f.write(reponse.content)
            print(f"OK {source_key} téléchargé et stocké")
        except Exception as error :
            print(f"X Echec de l'écriture pour {source_key}")
    else :
        print(f"X Echec du téléchargement pour {source_key} : {reponse.status_code}")

if __name__ == "__main__" :
    for key,info in Data_sources.items() :
        collect_data_to_bronze(key,info)
