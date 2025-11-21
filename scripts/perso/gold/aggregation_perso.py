import os
import pandas as pd

script_dir = os.path.dirname(os.path.abspath(__file__))
silver = os.path.abspath(os.path.join(script_dir, "../../../data/perso/silver"))
gold = os.path.abspath(os.path.join(script_dir, "../../../data/perso/gold"))

os.makedirs(gold, exist_ok=True)

def aggregation():
    print("=== Agrégation GOLD ===")

    # Charger les fichiers Silver
    df_ev = pd.read_csv(os.path.join(silver, "espaces_verts_silver.csv"))
    df_ar = pd.read_csv(os.path.join(silver, "arbres_silver.csv"))

    # Fusion sur la colonne Arrondissement
    df_final = df_ev.merge(df_ar, on="Arrondissement", how="outer")

    # Export Gold
    output_path = os.path.join(gold, "indicateurs_perso_gold.csv")
    df_final.to_csv(output_path, index=False)

    print(f"[OK] Gold créé → {output_path}")

if __name__ == "__main__":
    aggregation()
