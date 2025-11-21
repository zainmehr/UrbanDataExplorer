import os
import requests
import zipfile
from requests.exceptions import RequestException



# --- Configuration des chemins ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
chemin_bronze = os.path.join(BASE_DIR, "data", "bronze")


# Assurer que le dossier Bronze existe
os.makedirs(chemin_bronze, exist_ok=True)
print(f"Dossier Bronze: {chemin_bronze}")

# --- Sources de données (avec votre DVF et Logements sociaux activés) ---
Data_sources = {
    "dvf": {
        "url" : "https://static.data.gouv.fr/resources/demandes-de-valeurs-foncieres-geolocalisees/20251105-140205/dvf.csv.gz",
        "format":"csv",
        "filename":"valeurs_foncieres.csv.gz"
    },
    "logement_sociaux" : {
        "url" : "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/logements-sociaux-finances-a-paris/exports/csv?use_labels=true",
        "format":"csv",
        "filename":"logements_sociaux.csv"
    },
    "insee_logement" : {
        "url" : "https://www.insee.fr/fr/statistiques/fichier/8202349/base-cc-logement-2021_csv.zip",
        "format": "zip", 
        "filename": "recensement_logement.zip" 
    }
}

def collect_data_to_bronze(source_key, source_info) :
    url = source_info["url"]
    filename = source_info["filename"]
    data_format = source_info.get("format")
    
    chemin_destination = os.path.join(chemin_bronze, filename)
    print(f"\n-> Tentative de téléchargement: {source_key}")

    try :
        # Utilisation de stream=True pour les gros fichiers (comme DVF)
        reponse = requests.get(url, stream=True)
        # Lève une exception pour les codes 4xx ou 5xx
        reponse.raise_for_status() 

        # Écriture du contenu
        with open(chemin_destination, "wb") as f :
            for chunk in reponse.iter_content(chunk_size=8192): 
                f.write(chunk)
        print(f"  [OK] Fichier brut téléchargé: {filename}")

        # Décompression si c'est un ZIP (traitement spécifique au fichier de l'INSEE)
        if data_format == "zip" :
            print("  [INFO] Décompression du fichier ZIP...")
            with zipfile.ZipFile(chemin_destination, 'r') as zip_ref:
                zip_ref.extractall(chemin_bronze)
            print("  [OK] Décompression terminée.")

    except RequestException as e :
        print(f"  [X] Échec du téléchargement/réseau pour {source_key}: {e}")
    except Exception as e :
        print(f"  [X] Erreur inattendue pour {source_key}: {e}")

if __name__ == "__main__" :
    for key,info in Data_sources.items() :
        collect_data_to_bronze(key,info)