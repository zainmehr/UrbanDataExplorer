import os
import pandas as pd
import json

# ====================================================================
# --- Chemins et Configuration ---
# ====================================================================

# Définir le répertoire de base (UrbanExplorerProject/) en remontant 2 niveaux
# (scripts/api/ -> scripts/ -> UrbanExplorerProject/)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) )

# Chemins de lecture
GOLD_FILE = os.path.join(BASE_DIR, "data", "gold", "paris_logement_gold.csv")
GEOJSON_FILE = os.path.join(BASE_DIR, "data", "static", "arrondissements.geojson") 

# Stockage des données chargées
df_gold = pd.DataFrame()
geojson_data = {}

# Période de données confirmée (2020-2025)
DEFAULT_YEAR = 2025 
START_YEAR = 2020

# ====================================================================
# --- Fonction de Chargement ---
# ====================================================================

def load_data():
    """ Charge la table Gold et le GeoJSON en mémoire. """
    global df_gold, geojson_data
    
    # --- Chargement de la table Gold ---
    try:
        df_gold = pd.read_csv(GOLD_FILE)
        df_gold['Arrondissement'] = df_gold['Arrondissement'].astype(int)
        
        global DEFAULT_YEAR, START_YEAR
        if not df_gold.empty:
             DEFAULT_YEAR = df_gold['annee_mutation'].max()
             START_YEAR = df_gold['annee_mutation'].min()

        print(f"✅ Data Gold chargée ({len(df_gold)} lignes). Années : {START_YEAR}-{DEFAULT_YEAR}")

    except Exception as e:
        print(f"❌ Erreur critique lors du chargement de {GOLD_FILE} : {e}")
    
    # --- Chargement du GeoJSON ---
    try:
        with open(GEOJSON_FILE, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        print(f"✅ GeoJSON chargé.")
    except Exception as e:
        print(f"⚠️ AVERTISSEMENT : Erreur lors du chargement de {GEOJSON_FILE}. Assurez-vous qu'il est dans data/static/.")

# Charger les données lors de l'importation de utils
load_data()