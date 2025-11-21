import os
import pandas as pd
import numpy as np 


# --- Chemins et Paramètres ---

# Définir le répertoire de base (projet/ UrbanExplorerProject)
# On remonte le répertoire actuel (scripts/silver) de deux niveaux
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Chemins absolus
# Le chemin vers le dossier bronze est BASE_DIR + data/bronze
chemin_bronze = os.path.join(BASE_DIR, "data", "bronze")
chemin_silver = os.path.join(BASE_DIR, "data", "silver")
os.makedirs(chemin_silver, exist_ok=True) 

# Noms des fichiers
DVF_BRONZE = os.path.join(chemin_bronze, "valeurs_foncieres.csv.gz")
DVF_SILVER = os.path.join(chemin_silver, "dvf_transactions_silver.csv") 

LOGEMENT_BRONZE = os.path.join(chemin_bronze, "logements_sociaux.csv")
LOGEMENT_SILVER = os.path.join(chemin_silver, "logements_sociaux_silver.csv") 

INSEE_CSV_NAME = "base-cc-logement-2021.CSV" 
INSEE_BRONZE_CSV = os.path.join(chemin_bronze, INSEE_CSV_NAME) 
INSEE_SILVER = os.path.join(chemin_silver, "insee_logement_silver.csv")

# Constantes de filtrage
CODE_DEPT_PARIS = '75'

# Ces deux paramètres nous permettent de supprimer les valeurs absurdes
PLAGE_PRIX_M2_MIN = 3000   # Prix minimum/m² réaliste pour un appartement à Paris
PLAGE_PRIX_M2_MAX = 40000  # Prix maximum/m² réaliste


# --- Nettoyage et préparation DVF ---


def nettoyer_dvf() -> pd.DataFrame:
    """ Nettoie les données DVF, filtre sur Paris/ventes de logements, 
        calcule le prix/m² et filtre les outliers. """
    print("\n--- DEBUT NETTOYAGE DVF (Valeurs Foncières) ---")
    
    try:
        df = pd.read_csv(
            DVF_BRONZE,
            compression='gzip',
            sep=',',
            # Sélection des colonnes pertinentes
            usecols=[
                'code_departement', 'code_commune', 'date_mutation', 'nature_mutation', 
                'nombre_lots', 'type_local', 'surface_reelle_bati', 
                'valeur_fonciere', 'nombre_pieces_principales'
            ],
            dtype={'code_departement': str, 'code_commune': str, 'valeur_fonciere': str}, 
            low_memory=False
        )
        print(f"1. CHARGEMENT BRONZE DVF : {len(df):,} lignes.")
    except Exception as e:
        print(f"X Erreur lors du chargement du DVF. Assurez-vous que {DVF_BRONZE} existe. Détail : {e}")
        return pd.DataFrame()

    # 2. Suppression des doublons
    rows_before_drop = len(df)
    df.drop_duplicates(inplace=True)
    rows_after_drop = len(df)
    print(f"2. SUPPRESSION DES DOUBLONS : {rows_before_drop - rows_after_drop} doublons supprimés.")


    # 3. NORMALISATION DES TYPES et Calcul de l'Année
    df['valeur_fonciere'] = pd.to_numeric(df['valeur_fonciere'].str.replace(',', '.'), errors='coerce')
    df['surface_reelle_bati'] = pd.to_numeric(df['surface_reelle_bati'], errors='coerce')
    df['date_mutation'] = pd.to_datetime(df['date_mutation'], errors='coerce')
    df['annee_mutation'] = df['date_mutation'].dt.year

    # 4. FILTRAGE INITIAL (Paris, Ventes de logements, Lot unique, Année valide)
    df_clean = df[
        (df['code_departement'] == CODE_DEPT_PARIS) & 
        (df['nature_mutation'] == 'Vente') &
        (df['nombre_lots'] == 1) &
        (df['type_local'].isin(['Appartement', 'Maison'])) &
        (df['annee_mutation'].notna())
    ].copy()
    print(f"4. FILTRAGE Paris/Vente/Logement : {len(df_clean):,} lignes restantes.")
    
    
    # 5. CRÉATION DES COLONNES CLÉS
    df_clean['Arrondissement'] = df_clean['code_commune'].str[-2:].astype(int)
    
    # Calcul du prix au m² (gestion de la division par zéro)
    df_clean['prix_m2'] = np.where(df_clean['surface_reelle_bati'] > 0, 
                                df_clean['valeur_fonciere'] / df_clean['surface_reelle_bati'], 
                                np.nan)

    # 6. FILTRAGE FINAL DES VALEURS ABERRANTES et manquantes
    df_clean = df_clean[
        (df_clean['prix_m2'].between(PLAGE_PRIX_M2_MIN, PLAGE_PRIX_M2_MAX)) &
        (df_clean['nombre_pieces_principales'].notna()) &
        (df_clean['surface_reelle_bati'] > 5) 
    ].copy()
    print(f"6. FILTRAGE Outliers/NA : {len(df_clean):,} lignes finales.")


    # 7. SAUVEGARDE EN COUCHE SILVER
    colonnes_silver = ['Arrondissement', 'annee_mutation', 'prix_m2', 'type_local', 'nombre_pieces_principales']
    df_clean[colonnes_silver].to_csv(DVF_SILVER, index=False, encoding='utf-8')
    print(f"7. SAUVEGARDE SILVER DVF : {DVF_SILVER} (OK)")
    
    return df_clean


# --- B. Nettoyage Logement Sociaux (Part de logements sociaux) ---

def nettoyer_logement() -> pd.DataFrame:
    """ Nettoie les données de financement de logements sociaux par arrondissement. """
    print("\n--- DEBUT NETTOYAGE LOGEMENT (Logements Sociaux) ---")
    
    try:
        df = pd.read_csv(
            LOGEMENT_BRONZE,
            sep=';', 
            dtype={'Arrondissement': str} 
        )
        print(f"1. CHARGEMENT BRONZE LOGEMENT : {len(df):,} lignes.")
    except Exception as e:
        print(f"X Erreur lors du chargement du logement. Assurez-vous que {LOGEMENT_BRONZE} existe. Détail : {e}")
        return pd.DataFrame()

    # 2. Suppression des doublons
    rows_before_drop = len(df)
    df.drop_duplicates(inplace=True)
    rows_after_drop = len(df)
    print(f"2. SUPPRESSION DES DOUBLONS : {rows_before_drop - rows_after_drop} doublons supprimés.")

    # 3. SÉLECTION ET VÉRIFICATION DES COLONNES
    REQUIRED_COLS = ['Arrondissement', 'Nombre total de logements financés']
    if not all(col in df.columns for col in REQUIRED_COLS):
         print(f"X Colonnes requises manquantes : {REQUIRED_COLS}")
         return pd.DataFrame()
         
    df_clean = df[REQUIRED_COLS].copy()
    
    # Normalisation de la clé Arrondissement et Filtrage (1 à 20)
    df_clean['Arrondissement'] = pd.to_numeric(df_clean['Arrondissement'], errors='coerce')

    df_clean = df_clean[
        (df_clean['Arrondissement'].notna()) & 
        (df_clean['Arrondissement'].between(1, 20))
    ].copy()
    print(f"4. FILTRAGE ARRONDISSEMENT 1-20 : {len(df_clean):,} lignes restantes.")

    # 5. RENOMMAGE FINAL et Conversion en Numérique
    df_clean = df_clean.rename(
        columns={'Nombre total de logements financés': 'nb_logmt_soc_finance'}
    )
    df_clean['nb_logmt_soc_finance'] = pd.to_numeric(df_clean['nb_logmt_soc_finance'], errors='coerce').fillna(0)

    # 6. SAUVEGARDE EN COUCHE SILVER
    df_clean[['Arrondissement', 'nb_logmt_soc_finance']].to_csv(LOGEMENT_SILVER, index=False, encoding='utf-8')
    print(f"6. SAUVEGARDE SILVER LOGEMENT : {LOGEMENT_SILVER} (OK)")

    return df_clean

# --- C. Nettoyage INSEE (Typologie du parc immobilier) ---

def nettoyer_insee_logement() -> pd.DataFrame:
    """ Nettoie et agrège les données INSEE par arrondissement pour la typologie des logements. """
    print("\n--- DEBUT NETTOYAGE INSEE LOGEMENT (Typologie) ---")
    
    # 1. Chargement du CSV (doit avoir été décompressé par l'étape Bronze)
    COLS_INSEE = [
        'CODGEO', 'P21_LOG', 'P21_RP_1P', 'P21_RP_2P', 'P21_RP_3P', 
        'P21_RP_4P', 'P21_RP_5PP', 'P21_MAISON', 'P21_APPART'
    ]
    try:
        df = pd.read_csv(
            INSEE_BRONZE_CSV,
            sep=';',
            dtype={'CODGEO': str},
            usecols=COLS_INSEE
        )
        print(f"1. CHARGEMENT BRONZE INSEE (CSV) : {len(df):,} lignes.")
    except Exception as e:
        print(f"X Erreur de chargement du CSV INSEE. Assurez-vous que {INSEE_BRONZE_CSV} existe. Détail : {e}")
        return pd.DataFrame()
    
    # 2. Suppression des doublons
    df.drop_duplicates(inplace=True) 

    # 3. FILTRAGE ET CRÉATION DE LA CLÉ Arrondissement
    df_clean = df[df['CODGEO'].str.startswith('751')].copy()
    df_clean['Arrondissement'] = pd.to_numeric(df_clean['CODGEO'].str[-2:], errors='coerce')
    print(f"3. FILTRAGE PARIS (751xx) : {len(df_clean):,} lignes restantes.")
    
    # 4. AGRÉGATION PAR ARRONDISSEMENT (Somme des indicateurs par typologie)
    cols_to_sum = [col for col in COLS_INSEE if col != 'CODGEO']
    df_aggregated = df_clean.groupby('Arrondissement')[cols_to_sum].sum().reset_index()

    # 5. RENOMMAGE FINAL
    df_aggregated = df_aggregated.rename(columns={
        'P21_LOG': 'nb_logmt_total_parc',
        'P21_RP_1P': 'nb_rp_1p',
        'P21_RP_2P': 'nb_rp_2p',
        'P21_RP_3P': 'nb_rp_3p',
        'P21_RP_4P': 'nb_rp_4p',
        'P21_RP_5PP': 'nb_rp_5pp',
        'P21_MAISON': 'nb_maisons_total',
        'P21_APPART': 'nb_appartements_total',
    })

    # 6. Sauvegarde en Couche Silver
    df_aggregated.to_csv(INSEE_SILVER, index=False, encoding='utf-8')
    print(f"6. SAUVEGARDE SILVER INSEE : {INSEE_SILVER} (OK)")
    
    return df_aggregated


if __name__ == "__main__":
    print("\n= DÉBUT DU PIPELINE NETTOYAGE (ZONE SILVER) =")
    
    df_dvf_silver = nettoyer_dvf()
    df_logement_silver = nettoyer_logement()
    df_insee_logement_silver = nettoyer_insee_logement()

    print("\n= FIN DU NETTOYAGE =")

    # Vérification des DataFrames
    if not df_dvf_silver.empty and not df_logement_silver.empty and not df_insee_logement_silver.empty:
        print("\n✅ Les DataFrames Silver sont prêts pour l'agrégation (Zone Gold).")
    else:
        print("\n❌ ATTENTION : Un des DataFrames est vide. Vérifiez les chemins et les filtres.")