import os
import pandas as pd
import numpy as np 

# --- Chemins et Paramètres ---
script_dir = os.path.dirname(os.path.abspath(__file__)) # Répertoire du script actuel
chemin_bronze_relatif = "../../data/bronze"
chemin_silver_relatif = "../../data/silver"

chemin_bronze = os.path.abspath(os.path.join(script_dir, chemin_bronze_relatif))
chemin_silver = os.path.abspath(os.path.join(script_dir, chemin_silver_relatif))
os.makedirs(chemin_silver, exist_ok=True) 

# Chemins Dataset 1: DVF
dvf_chemin_bronze = os.path.join(chemin_bronze,"valeurs_foncieres.csv.gz")
dvf_chemin_silver = os.path.join(chemin_silver,"dvf_transactions_silver.csv") 

# Chemins Dataset 2: Logements Sociaux
logement_chemin_bronze = os.path.join(chemin_bronze,"logements_sociaux.csv")
logement_chemin_silver = os.path.join(chemin_silver,"logements_sociaux_silver.csv") 

# Chemins Dataset 3: Filosofi (Revenus)
filosofi_data_bronze = os.path.join(chemin_bronze, "DS_FILOSOFI_CC_2021_data.csv")
filosofi_meta_bronze = os.path.join(chemin_bronze, "DS_FILOSOFI_CC_2021_metadata.csv")
filosofi_chemin_silver = os.path.join(chemin_silver, "filosofi_revenus_paris_silver.csv") 

# Chemins Dataset 4: Accidentologie
accident_chemin_bronze = os.path.join(chemin_bronze, "accidentologie0.csv")
accident_chemin_silver = os.path.join(chemin_silver, "accidentologie_paris_silver.csv")

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

def nettoyer_filosofi():
    """
    Nettoie les données Filosofi pour isoler le revenu médian
    de Paris (commune et arrondissements).
    """
    print("--- DEBUT NETTOYAGE FILOSOFI (Revenus) ---")
    
    # --- 1. Chargement des fichiers ---
    try:
        # Utilise les chemins définis en haut du script
        df_data = pd.read_csv(filosofi_data_bronze, sep=';', low_memory=False)
        df_meta = pd.read_csv(filosofi_meta_bronze, sep=';')
        print(f"1. CHARGEMENT BRONZE : {len(df_data):,} lignes (data) et {len(df_meta):,} lignes (meta).")
    except Exception as e:
        print(f"X Erreur lors du chargement des fichiers Filosofi : {e}")
        return pd.DataFrame()

    # --- 2. Préparation des filtres ---
    MESURE_REVENU_MEDIAN = 'MED_SL'
    
    # Codes géo (Paris commune + 20 arrondissements)
    arrondissement_codes = [f"751{i:02d}" for i in range(1, 21)]
    paris_codes_stricts = arrondissement_codes + ["75056"] # 75056 = Commune de Paris

    # Mapping des noms géographiques
    geo_mapping = df_meta[
        (df_meta['COD_VAR'] == 'GEO') &
        (df_meta['COD_MOD'].isin(paris_codes_stricts))
    ][['COD_MOD', 'LIB_MOD']].drop_duplicates(subset=['COD_MOD'])
    
    geo_mapping = geo_mapping.rename(columns={'COD_MOD': 'GEO', 'LIB_MOD': 'Nom_Geographie'})
    
    if len(geo_mapping) != 21:
        print(f"ATTENTION : {len(geo_mapping)} codes géo trouvés au lieu de 21. Vérifiez les métadonnées.")

    # --- 3. Nettoyage des données ---
    df_data['GEO'] = df_data['GEO'].astype(str)

    df_clean = df_data[
        (df_data['FILOSOFI_MEASURE'] == MESURE_REVENU_MEDIAN) &
        (df_data['GEO'].isin(paris_codes_stricts))
    ].copy()
    print(f"3. FILTRAGE (Revenu médian + Géo Paris) : {len(df_clean):,} lignes restantes.")
    
    df_clean = df_clean[df_clean['CONF_STATUS'] != 'C']
    df_clean = df_clean.drop_duplicates(subset=['GEO'])

    # --- 4. Jointure et finalisation ---
    df_final = df_clean.merge(geo_mapping, on='GEO', how='left')
    
    colonnes_silver = ['GEO', 'Nom_Geographie', 'OBS_VALUE', 'UNIT_MEASURE', 'TIME_PERIOD']
    df_final = df_final[colonnes_silver]
    
    df_final = df_final.rename(
        columns={'OBS_VALUE': 'Niveau_de_vie_median_EUR_AN'}
    )
    df_final = df_final.sort_values(by='GEO')
    
    print(f"4. FINALISATION : {len(df_final)} lignes prêtes (20 arr. + 1 commune).")

    # --- 5. Sauvegarde en couche Silver ---
    try:
        # Utilise le chemin de sortie silver défini en haut
        df_final.to_csv(filosofi_chemin_silver, index=False, encoding='utf-8', sep=';')
        print(f"5. SAUVEGARDE SILVER : {filosofi_chemin_silver} (OK)")
    except Exception as e:
        print(f"X Erreur lors de la sauvegarde Filosofi Silver : {e}")

    return df_final

def nettoyer_accidentologie():
    """
    Nettoie les données d'accidentologie et les agrège par
    arrondissement et par année.
    """
    print("--- DEBUT NETTOYAGE ACCIDENTOLOGIE ---")
    
    # --- 1. Chargement des fichiers ---
    try:
        # Utilise le chemin bronze défini en haut
        df = pd.read_csv(accident_chemin_bronze, sep=';', low_memory=False)
        print(f"1. CHARGEMENT BRONZE : {len(df):,} lignes.")
    except Exception as e:
        print(f"X Erreur lors du chargement du fichier accidentologie0.csv : {e}")
        return pd.DataFrame()

    # --- 2. Nettoyage et préparation ---
    
    # 2a. Extraction de l'année
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df['annee'] = df['Date'].dt.year
    
    # 2b. Normalisation de l'arrondissement
    # On utilise 'arronco' (ex: 75101) pour extraire le numéro (ex: 1)
    df['Arrondissement'] = pd.to_numeric(df['arronco'], errors='coerce')
    df['Arrondissement'] = df['Arrondissement'].astype(str).str[-2:].astype(float).astype('Int64')
    
    # 2c. Remplir les NaN par 0 pour les colonnes de comptage
    cols_gravite = ['Blessés Légers', 'Blessés hospitalisés', 'Tué']
    df[cols_gravite] = df[cols_gravite].fillna(0)

    # 2d. Filtrer les lignes invalides (pas d'année ou pas d'arrondissement)
    df_clean = df[
        (df['annee'].notna()) &
        (df['Arrondissement'].notna()) &
        (df['Arrondissement'].between(1, 20))
    ].copy()
    print(f"3. FILTRAGE (Année valide + Arr. 1-20) : {len(df_clean):,} lignes restantes.")

    # --- 3. Agrégation ---
    # On groupe par année et arrondissement, et on somme les victimes
    df_agg = df_clean.groupby(['annee', 'Arrondissement'])[cols_gravite].sum().reset_index()
    
    # Renommer les colonnes pour la couche Silver
    df_agg = df_agg.rename(columns={
        'Blessés Légers': 'nb_blesses_legers',
        'Blessés hospitalisés': 'nb_blesses_hospitalises',
        'Tué': 'nb_tues'
    })
    
    print(f"4. AGREGATION : {len(df_agg)} lignes agrégées (année/arrondissement).")

    # --- 4. Sauvegarde en couche Silver ---
    try:
        # Utilise le chemin de sortie silver défini en haut
        df_agg.to_csv(accident_chemin_silver, index=False, encoding='utf-8', sep=';')
        print(f"5. SAUVEGARDE SILVER : {accident_chemin_silver} (OK)")
    except Exception as e:
        print(f"X Erreur lors de la sauvegarde Accidentologie Silver : {e}")

    return df_agg

if __name__ == "__main__" :
    #os.makedirs(chemin_silver, exist_ok=True) #
    
    df_dvf_silver = nettoyer_dvf() 
    print("\n")
    df_logement_silver = nettoyer_logement() 
    print("\n")
    df_filosofi_silver = nettoyer_filosofi()
    print("\n")
    df_accident_silver = nettoyer_accidentologie()
    
    print("\n FIN DU NETTOYAGE ")

    # Vérification si les DataFrames ne sont pas vides
    if (not df_dvf_silver.empty if isinstance(df_dvf_silver, pd.DataFrame) else False) and \
       (not df_logement_silver.empty if isinstance(df_logement_silver, pd.DataFrame) else False) and \
       (not df_filosofi_silver.empty if isinstance(df_filosofi_silver, pd.DataFrame) else False) and \
       (not df_accident_silver.empty if isinstance(df_accident_silver, pd.DataFrame) else False):
        print("\n=> Les 4 DataFrames Silver sont prêts pour l'agrégation.")
    else:
        print("\n=> ATTENTION : Un des DataFrames est vide. Vérifiez les chemins Bronze et les filtres.")