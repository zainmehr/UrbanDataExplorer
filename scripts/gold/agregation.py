import os
import pandas as pd
import numpy as np

#1. Configuration des Chemins
script_dir = os.path.dirname(os.path.abspath(__file__))
chemin_silver_relatif = "../../data/silver"
chemin_gold_relatif = "../../data/gold"

chemin_silver = os.path.abspath(os.path.join(script_dir, chemin_silver_relatif))
chemin_gold = os.path.abspath(os.path.join(script_dir, chemin_gold_relatif))
os.makedirs(chemin_gold, exist_ok=True)

# Chemins des fichiers d'entrée (Silver)
path_dvf = os.path.join(chemin_silver, "dvf_transactions_silver.csv")
path_filosofi = os.path.join(chemin_silver, "filosofi_revenus_paris_silver.csv")
path_logement = os.path.join(chemin_silver, "logements_sociaux_silver.csv")
path_accidents = os.path.join(chemin_silver, "accidentologie_paris_silver.csv")

# Chemin du fichier de sortie (Gold)
path_output_csv = os.path.join(chemin_gold, "paris_donnees_completes_gold.csv")
path_output_json = os.path.join(chemin_gold, "paris_donnees_completes_gold.json")

def charger_donnees():
    print("--- Chargement des données Silver ---")
    try:
        df_dvf = pd.read_csv(path_dvf)
        df_filosofi = pd.read_csv(path_filosofi, sep=';')
        df_logement = pd.read_csv(path_logement)
        df_accidents = pd.read_csv(path_accidents, sep=';')
        print(" Tous les fichiers Silver sont chargés.")
        return df_dvf, df_filosofi, df_logement, df_accidents
    except FileNotFoundError as e:
        print(f" Erreur : Fichier manquant. {e}")
        return None, None, None, None

def transformation_dvf(df_dvf):
    print("--- Transformation DVF (Prix médian & Variation) ---")
    
    # Agregation par Année et Arrondissement
    df_agg = df_dvf.groupby(['annee_mutation', 'Arrondissement'])['prix_m2'].median().reset_index()
    df_agg = df_agg.rename(columns={'prix_m2': 'prix_m2_median'})
    
    # Calcul de la variation annuelle en pourcentage
    df_agg = df_agg.sort_values(by=['Arrondissement', 'annee_mutation'])
    df_agg['variation_prix_annuelle_pourc'] = df_agg.groupby('Arrondissement')['prix_m2_median'].pct_change() * 100
    
    # Arrondir
    df_agg['prix_m2_median'] = df_agg['prix_m2_median'].round(2)
    df_agg['variation_prix_annuelle_pourc'] = df_agg['variation_prix_annuelle_pourc'].round(2)
    
    print(f" DVF agrégé : {len(df_agg)} lignes (Année x Arrondissement).")
    return df_agg

def preparer_filosofi(df_filosofi):
    # Nettoyage spécifique pour Filosofi
    df_clean = df_filosofi.copy()
    # On filtre pour garder les arrondissements (codes commencant par 751)
    df_clean = df_clean[df_clean['GEO'].astype(str).str.startswith('751')]
    # On extrait le numéro (ex: 75101 -> 1)
    df_clean['Arrondissement'] = df_clean['GEO'].astype(str).str[-2:].astype(int)
    df_clean = df_clean[['Arrondissement', 'Niveau_de_vie_median_EUR_AN']]
    return df_clean

def preparer_logement(df_logement):
    print("--- Préparation Logements Sociaux ---")
    # On groupe par arrondissement et on SOMME le nombre de logements
    df_agg = df_logement.groupby('Arrondissement')['nb_logmt_total_finance'].sum().reset_index()
    
    # On renomme la colonne pour être clair
    df_agg = df_agg.rename(columns={'nb_logmt_total_finance': 'cumul_logements_sociaux_finances'})
    
    print(f" Logements agrégés : {len(df_agg)} arrondissements uniques.")
    return df_agg

def enrichissement(df_base, df_filosofi, df_logement, df_accidents):
    print("--- Enrichissement (Fusion des tables) ---")
    
    # 1. Fusion avec les Accidents (Clés : Année & Arrondissement)
    df_merged = pd.merge(
        df_base, 
        df_accidents, 
        left_on=['annee_mutation', 'Arrondissement'], 
        right_on=['annee', 'Arrondissement'], 
        how='left'
    )
    
    # 2. Fusion avec Filosofi (Clé : Arrondissement)
    df_merged = pd.merge(df_merged, df_filosofi, on='Arrondissement', how='left')
    
    # 3. Fusion avec Logements Sociaux (Clé : Arrondissement)
    df_merged = pd.merge(df_merged, df_logement, on='Arrondissement', how='left')
    
    # Nettoyage post-fusion
    df_merged = df_merged.drop(columns=['annee'])
    # Remplir les NaN des accidents par 0 (s'il n'y a pas eu d'accidents, on met 0)
    cols_accidents = ['nb_blesses_legers', 'nb_blesses_hospitalises', 'nb_tues']
    df_merged[cols_accidents] = df_merged[cols_accidents].fillna(0)
    
    if 'cumul_logements_sociaux_finances' in df_merged.columns:
        df_merged['cumul_logements_sociaux_finances'] = df_merged['cumul_logements_sociaux_finances'].fillna(0)
    
    print(f" Table finale créée : {df_merged.shape[0]} lignes x {df_merged.shape[1]} colonnes.")
    return df_merged

if __name__ == "__main__":
    print("===== DÉBUT DE L'AGRÉGATION GOLD =====")
    
    # 1. Chargement
    dvf, filosofi, logement, accidents = charger_donnees()
    
    if dvf is not None:
        # 2. Transformation
        dvf_agg = transformation_dvf(dvf)
        filosofi_clean = preparer_filosofi(filosofi)
        
        
        logement_agg = preparer_logement(logement)
        
        
        # 3. Enrichissement
        df_final = enrichissement(dvf_agg, filosofi_clean, logement_agg, accidents)
        
        # 4. Export
        print("--- Export des données ---")
        df_final.to_csv(path_output_csv, index=False, sep=';', encoding='utf-8')
        print(f" CSV sauvegardé : {path_output_csv}")
        
        df_final.to_json(path_output_json, orient='records', force_ascii=False, indent=4)
        print(f" JSON sauvegardé : {path_output_json}")
        
        print("\n TERMINÉ ! Le dataset prêt pour la dataviz est dans data/gold.")