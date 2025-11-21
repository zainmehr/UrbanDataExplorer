import os
import pandas as pd

# Dossiers
script_dir = os.path.dirname(os.path.abspath(__file__))
bronze = os.path.abspath(os.path.join(script_dir, "../../../data/perso/bronze"))
silver = os.path.abspath(os.path.join(script_dir, "../../../data/perso/silver"))

os.makedirs(silver, exist_ok=True)

def nettoyer_espaces_verts():
    print("=== Nettoyage Espaces Verts ===")

    # Charger le CSV brut
    df = pd.read_csv(os.path.join(bronze, "espaces_verts.csv"), sep=";")

    # Extraire l'arrondissement à partir du code postal (ex: 75011 → 11)
    df["Arrondissement"] = df["adresse_codepostal"].astype(str).str[-2:].astype(int)

    # On garde seulement ce qu'on a besoin
    df_clean = df[["Arrondissement", "poly_area"]].copy()

    # On somme les surfaces d'espaces verts par arrondissement
    agg = df_clean.groupby("Arrondissement")["poly_area"].sum().reset_index()

    # Renommer la colonne
    agg.columns = ["Arrondissement", "Surface_espaces_verts_m2"]

    # Export en Silver
    output_path = os.path.join(silver, "espaces_verts_silver.csv")
    agg.to_csv(output_path, index=False)
    print(f"[OK] Silver créé → {output_path}")

if __name__ == "__main__":
    nettoyer_espaces_verts()
