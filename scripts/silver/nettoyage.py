import os
import pandas as pd
import numpy as np 
import zipfile

# --- Chemins et Paramètres ---
chemin_bronze_relatif = "../../data/bronze"
chemin_silver_relatif = "../../data/silver"

chemin_bronze = os.path.abspath(chemin_bronze_relatif)
chemin_silver = os.path.abspath(chemin_silver_relatif)
os.makedirs(chemin_silver, exist_ok=True) 

dvf_chemin_bronze = os.path.join(chemin_bronze,"valeurs_foncieres.csv.gz")
dvf_chemin_silver = os.path.join(chemin_silver,"dvf_transactions_silver.csv") 

logement_chemin_bronze = os.path.join(chemin_bronze,"logements_sociaux.csv")
logement_chemin_silver = os.path.join(chemin_silver,"logements_sociaux_silver.csv") 

insee_chemin_bronze_zip = os.path.join(chemin_bronze, "recensement_logement.zip")
insee_chemin_silver = os.path.join(chemin_silver, "insee_logement_silver.csv")
INSEE_CSV_NAME = "base-cc-logement-2021.CSV" 
insee_chemin_temp_csv = os.path.join(chemin_bronze, INSEE_CSV_NAME)




code_paris = '75'


def nettoyer_dvf() :
    print("DEBUT NETTOYAGE DVF")
    
    try:
        df = pd.read_csv(
            dvf_chemin_bronze,
            compression='gzip',
            sep=',',
            dtype={'code_departement': str, 'code_commune': str}, 
            low_memory=False,
            usecols=[
                'code_departement', 'code_commune', 'date_mutation', 'nature_mutation', 
                'nombre_lots', 'type_local', 'surface_reelle_bati', 
                'valeur_fonciere', 'nombre_pieces_principales'
            ]
        )
        print(f"1. CHARGEMENT BRONZE DVF : {len(df):,} lignes.")
    except Exception as e:
        print(f"X Erreur lors du chargement du DVF : {e}")
        return pd.DataFrame()

    rows_before_drop = len(df)
    df.drop_duplicates(inplace=True)
    rows_after_drop = len(df)
    print(f"2. SUPPRESSION DES DOUBLONS : {rows_before_drop - rows_after_drop} doublons supprimés.")


    # 3. FILTRAGE INITIAL ET NORMALISATION DES TYPES
    df_clean = df[
            (df['code_departement'] == code_paris) & 
            (df['nature_mutation'] == 'Vente') &
            (df['nombre_lots'] == 1)
        ].copy()
    print(f"3. FILTRAGE PARIS/VENTE/LOT UNIQ : {len(df_clean):,} lignes restantes.")
    
    df_clean['date_mutation'] = pd.to_datetime(df_clean['date_mutation'], errors='coerce')
    df_clean['annee_mutation'] = df_clean['date_mutation'].dt.year

    df_clean['valeur_fonciere'] = pd.to_numeric(df_clean['valeur_fonciere'], errors='coerce')
    df_clean['surface_reelle_bati'] = pd.to_numeric(df_clean['surface_reelle_bati'], errors='coerce')

    df_clean = df_clean[
        (df_clean['valeur_fonciere'].notna()) & 
        (df_clean['surface_reelle_bati'].notna()) 
        ].copy()
    print(f"4. FILTRAGE VALEUR/SURFACE NULLES : {len(df_clean):,} lignes finales.")


    # 4. CREATION DES COLONNES Arrondissement et prix_m2
    df_clean['Arrondissement'] = df_clean['code_commune'].str[-2:].astype(int)
    df_clean['prix_m2'] = df_clean['valeur_fonciere'] / df_clean['surface_reelle_bati']

    # 5. SAUVEGARDE EN COUCHE SILVER
    colonnes_silver = ['Arrondissement', 'annee_mutation', 'prix_m2', 'type_local', 'nombre_pieces_principales']
    df_clean[colonnes_silver].to_csv(dvf_chemin_silver, index=False, encoding='utf-8')
    print(f"5. SAUVEGARDE SILVER : {dvf_chemin_silver} (OK)")
    
    return df_clean

def nettoyer_logement() :
    print("DEBUT NETTOYAGE LOGEMENT")
    
    try:
        df = pd.read_csv(
            logement_chemin_bronze,
            sep=';',
        )
        print(f"1. CHARGEMENT BRONZE LOGEMENT : {len(df):,} lignes.")
    except Exception as e:
        print(f"X Erreur lors du chargement du logement : {e}")
        return pd.DataFrame()

    # 2. Suppression des doublons
    rows_before_drop = len(df)
    df.drop_duplicates(inplace=True)
    rows_after_drop = len(df)
    print(f"2. SUPPRESSION DES DOUBLONS : {rows_before_drop - rows_after_drop} doublons supprimés.")


    # 3. FILTRAGE ET RENOMMAGE
    df_clean = df[['Arrondissement','Nombre total de logements financés']].copy()
    
    
    df_clean['Arrondissement'] = pd.to_numeric(df_clean['Arrondissement'], errors='coerce')

    # 4. FILTRAGE DES ARRONDISSEMENTS VALIDES
    df_clean = df_clean[
        (df_clean['Arrondissement'].notna()) & 
        (df_clean['Arrondissement'].between(1, 20))
    ]
    print(f"4. FILTRAGE ARRONDISSEMENT 1-20 : {len(df_clean):,} lignes restantes.")

    # 5. RENOMMAGE FINAL
    df_clean = df_clean.rename(
        columns={'Nombre total de logements financés': 'nb_logmt_total_finance'}
    )

    # 6. SAUVEGARDE EN COUCHE SILVER
    df_clean.to_csv(logement_chemin_silver, index=False, encoding='utf-8')
    print(f"6. SAUVEGARDE SILVER : {logement_chemin_silver} (OK)")

    return df_clean

def nettoyer_insee_logement():
    print("\nDEBUT NETTOYAGE INSEE LOGEMENT")
    
    # 1. Décompression du fichier ZIP
    try:
        with zipfile.ZipFile(insee_chemin_bronze_zip, 'r') as zip_ref:
            # Extrait le fichier CSV dans le même répertoire que le ZIP (chemin_bronze)
            zip_ref.extract(INSEE_CSV_NAME, path=chemin_bronze)
        print(f"1. DÉCOMPRESSION : {INSEE_CSV_NAME} extrait.")
    except FileNotFoundError:
        print(f"X Fichier ZIP INSEE non trouvé : {insee_chemin_bronze_zip}. Vérifiez l'ingestion.")
        return pd.DataFrame()
    except Exception as e:
        print(f"X Erreur lors de la décompression du ZIP : {e}")
        return pd.DataFrame()

    # 2. Chargement du CSV décompressé
    try:
        df = pd.read_csv(
            insee_chemin_temp_csv,
            sep=';',
            dtype={'CODGEO': str},
            usecols=[
                'CODGEO',
                'P21_LOG', 'P21_RP_1P', 'P21_RP_2P', 'P21_RP_3P', 
                'P21_RP_4P', 'P21_RP_5PP', 'P21_MAISON', 'P21_APPART'
            ]
        )
        print(f"2. CHARGEMENT BRONZE INSEE : {len(df):,} lignes.")
    except Exception as e:
        print(f"X Erreur de chargement du CSV INSEE : {e}")
        return pd.DataFrame()
    
    # 3. Suppression des doublons
    rows_before_drop = len(df)
    df.drop_duplicates(inplace=True) 
    rows_after_drop = len(df)
    print(f"3. SUPPRESSION DES DOUBLONS : {rows_before_drop - rows_after_drop} doublons supprimés.")

    # 4. FILTRAGE ET CRÉATION DE LA CLÉ Arrondissement
    # Filtrer uniquement les arrondissements de Paris (CODGEO commence par 751)
    df_clean = df[df['CODGEO'].str.startswith('751')].copy()
    
    # Création de la clé Arrondissement
    df_clean['Arrondissement'] = pd.to_numeric(df_clean['CODGEO'].str[-2:], errors='coerce')
    print(f"4. FILTRAGE PARIS : {len(df_clean):,} lignes restantes (IRIS ou Arrondissements).")
    
    # 5. RENOMMAGE FINAL (Standardisation pour la Tâche 3)
    df_clean = df_clean.rename(columns={
        'P21_LOG': 'nb_logmt_total_parc', # Total du parc immobilier
        'P21_RP_1P': 'rp_1p',
        'P21_RP_2P': 'rp_2p',
        'P21_RP_3P': 'rp_3p',
        'P21_RP_4P': 'rp_4p',
        'P21_RP_5PP': 'rp_5pp',
        'P21_MAISON': 'nb_maisons_total',
        'P21_APPART': 'nb_appartements_total',
    }).drop(columns=['CODGEO'])

    # 6. Sauvegarde en Couche Silver
    df_clean.to_csv(insee_chemin_silver, index=False, encoding='utf-8')
    print(f"6. SAUVEGARDE SILVER INSEE : {insee_chemin_silver} (OK)")
    
    return df_clean



if __name__ == "__main__" :
    os.makedirs(chemin_silver, exist_ok=True) 
    
    # df_dvf_silver = nettoyer_dvf()
    # df_logement_silver = nettoyer_logement()
    df_insee_logement = nettoyer_insee_logement()

    print("\n FIN DU NETTOYAGE ")

    # Vérification si les DataFrames ne sont pas vides
    # if not df_dvf_silver.empty and not df_logement_silver.empty and not df_insee_logement.empty:
    #     print("\n Les DataFrames Silver sont prêts pour l'agrégation (Tâche 3).")
    # else:
    #     print("\nATTENTION : Un des DataFrames est vide. Vérifiez les chemins Bronze et les filtres.")