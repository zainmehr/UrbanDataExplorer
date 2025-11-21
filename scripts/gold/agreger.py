import os
import pandas as pd
import numpy as np


# --- Chemins et Paramètres ---

# Définir le répertoire de base (UrbanExplorerProject/)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) 

# Chemins de lecture (Silver)
chemin_silver = os.path.join(BASE_DIR, "data", "silver")
DVF_SILVER = os.path.join(chemin_silver, "dvf_transactions_silver.csv") 
LOGEMENT_SILVER = os.path.join(chemin_silver, "logements_sociaux_silver.csv") 
INSEE_SILVER = os.path.join(chemin_silver, "insee_logement_silver.csv")

# Chemin d'écriture (Gold)
chemin_gold = os.path.join(BASE_DIR, "data", "gold")
os.makedirs(chemin_gold, exist_ok=True) 
GOLD_FINAL = os.path.join(chemin_gold, "paris_logement_gold.csv")


# --- A. CALCUL DES INDICATEURS DVF (Prix/Évolution) ---

def aggregate_dvf(df_dvf: pd.DataFrame) -> pd.DataFrame:
    """ Calcule le prix/m² médian et sa variation annuelle par arrondissement et par année (Exigence : Prix/m² médian, Évolution temporelle). """
    print("-> 1. Agrégation DVF : Prix Médian et Évolution...")

    # 1. Calcul du prix médian/m² par Arrondissement et Année 
    df_prix = df_dvf.groupby(['Arrondissement', 'annee_mutation'])['prix_m2'].median().reset_index()
    df_prix = df_prix.rename(columns={'prix_m2': 'prix_m2_median'})
    
    # 2. Calcul de la variation annuelle (%) 
    df_prix = df_prix.sort_values(by=['Arrondissement', 'annee_mutation'])
    
    # pct_change() calcule la variation en % par rapport à la ligne précédente
    df_prix['var_an_pct'] = df_prix.groupby('Arrondissement')['prix_m2_median'].pct_change() * 100
    
    # Colonne pour le storytelling (ex: '2022' vs '2023')
    df_prix['prix_m2_median_an_prec'] = df_prix.groupby('Arrondissement')['prix_m2_median'].shift(1)
    
    print(f"  [OK] DVF agrégé sur {len(df_prix):,} lignes (Historique Arrondissement x Année).")
    return df_prix

# --- B. CALCUL DES INDICATEURS INSEE (Typologie) ---

def calculate_insee_typology(df_insee: pd.DataFrame) -> pd.DataFrame:
    """ Calcule la répartition en pourcentage du parc immobilier par typologie (Exigence : Typologie des logements). """
    print("-> 2. Calcul INSEE : Répartition du Parc Immobilier...")

    df_typo = df_insee.copy()
    
    # Calcul du total des Résidences Principales (somme des T1 à T5+)
    df_typo['nb_rp_total'] = df_typo[['nb_rp_1p', 'nb_rp_2p', 'nb_rp_3p', 'nb_rp_4p', 'nb_rp_5pp']].sum(axis=1)

    # Créer le total pour T4 et + (car souvent affiché ainsi)
    df_typo['nb_rp_4p_et_plus'] = df_typo['nb_rp_4p'] + df_typo['nb_rp_5pp']
    
    # Calcul des pourcentages par rapport au total des résidences principales (RP)
    df_typo['part_rp_1p_pct'] = (df_typo['nb_rp_1p'] / df_typo['nb_rp_total']) * 100
    df_typo['part_rp_2p_pct'] = (df_typo['nb_rp_2p'] / df_typo['nb_rp_total']) * 100
    df_typo['part_rp_3p_pct'] = (df_typo['nb_rp_3p'] / df_typo['nb_rp_total']) * 100
    df_typo['part_rp_4p_et_plus_pct'] = (df_typo['nb_rp_4p_et_plus'] / df_typo['nb_rp_total']) * 100


    # Pourcentage de Maisons par rapport au Parc total
    df_typo['part_maisons_pct'] = (df_typo['nb_maisons_total'] / df_typo['nb_logmt_total_parc']) * 100

    # Colonnes finales pour la fusion
    cols_gold_insee = [
        'Arrondissement', 'nb_logmt_total_parc', 
        'part_rp_1p_pct', 'part_rp_2p_pct', 'part_rp_3p_pct', 'part_rp_4p_et_plus_pct',
        'part_maisons_pct'
    ]
    
    print(f"  [OK] Typologie calculée sur {len(df_typo):,} arrondissements.")
    return df_typo[cols_gold_insee]

# --- C. CALCUL DES LOGEMENTS SOCIAUX (%) ---

def calculate_social_housing(df_logement: pd.DataFrame, df_insee: pd.DataFrame) -> pd.DataFrame:
    """ Calcule le pourcentage de logements sociaux par rapport au parc total (INSEE) (Exigence : Logements sociaux (%)). """
    print("-> 3. Calcul Logements Sociaux : Pourcentage du Parc Total...")

    # 1. Agrégation des financements (le fichier silver peut contenir plusieurs lignes par arrondissement/année de financement)
    df_soc_agg = df_logement.groupby('Arrondissement')['nb_logmt_soc_finance'].sum().reset_index()

    # 2. Fusion avec le total du parc (issu de l'INSEE)
    df_insee_total = df_insee[['Arrondissement', 'nb_logmt_total_parc']]
    df_merge = pd.merge(df_soc_agg, df_insee_total, on='Arrondissement', how='left')

    # 3. Calcul du pourcentage (Total Logements Sociaux Financés / Total Logements Parc)
    df_merge['part_logmt_sociaux_pct'] = np.where(
        df_merge['nb_logmt_total_parc'] > 0,
        (df_merge['nb_logmt_soc_finance'] / df_merge['nb_logmt_total_parc']) * 100,
        np.nan
    )
    
    cols_gold_soc = ['Arrondissement', 'part_logmt_sociaux_pct']
    
    print(f"  [OK] Part Logements Sociaux calculée sur {len(df_merge):,} arrondissements.")
    return df_merge[cols_gold_soc]


# --- D. ORCHESTRATION ET FUSION FINALE (Création de la table Gold) ---

def run_gold_pipeline():
    """ Lit les données Silver, calcule tous les indicateurs, les fusionne et crée la table Gold finale. """
    print("\n= DÉBUT DU PIPELINE AGRÉGATION (ZONE GOLD) =")
    
    try:
        # Lecture des fichiers Silver
        df_dvf = pd.read_csv(DVF_SILVER)
        df_logement = pd.read_csv(LOGEMENT_SILVER)
        df_insee = pd.read_csv(INSEE_SILVER)
        print("-> Chargement des trois sources Silver réussi.")
    except Exception as e:
        print(f"X Échec du chargement des fichiers Silver. Vérifiez l'étape précédente. Détail : {e}")
        return

    # 1. Calcul des indicateurs DVF (Prix/Évolution)
    df_dvf_agg = aggregate_dvf(df_dvf)
    
    # 2. Calcul des indicateurs INSEE (Typologie)
    df_insee_agg = calculate_insee_typology(df_insee)
    
    # 3. Calcul des Logements Sociaux (%)
    df_soc_agg = calculate_social_housing(df_logement, df_insee_agg) # Utilise la sortie de l'étape 2

    # 4. Fusion des DataFrames (Exigence : Fusionner les jeux de données)
    print("\n-> 4. Fusion des indicateurs pour la table Gold (Arrondissement x Année)...")
    
    # Fusion A: DVF (historique) + Logement Sociaux (%)
    df_gold_final = pd.merge(df_dvf_agg, df_soc_agg, on='Arrondissement', how='left')
    
    # Fusion B: Résultat + Typologie INSEE
    # On fusionne sur Arrondissement car la typologie (2021) et la part des logements sociaux sont statiques pour chaque Arrondissement/Année dans cette vue analytique.
    df_gold_final = pd.merge(df_gold_final, df_insee_agg.drop(columns=['nb_logmt_total_parc']), on='Arrondissement', how='left')
    
    # Nettoyage final (arrondi des pourcentages)
    df_gold_final = df_gold_final.round({
        'prix_m2_median': 0,
        'var_an_pct': 2,
        'part_logmt_sociaux_pct': 2,
        'part_rp_1p_pct': 2,
        'part_rp_2p_pct': 2,
        'part_rp_3p_pct': 2,
        'part_rp_4p_et_plus_pct': 2,
        'part_maisons_pct': 2,
    })
    
    # 5. Sauvegarde en Couche Gold (Exigence : Exporter au format CSV) 
    df_gold_final.to_csv(GOLD_FINAL, index=False, encoding='utf-8')
    print(f"\n5. SAUVEGARDE GOLD FINALE : {GOLD_FINAL} (OK)")
    print(f"   Table analytique créée (Historique Arrondissement x Année).")
    
    print("\n= FIN DU PIPELINE (ZONE GOLD COMPLÉTÉE) =")


if __name__ == "__main__":
    run_gold_pipeline()