import os
import pandas as pd

script_dir = os.path.dirname(os.path.abspath(__file__))
bronze = os.path.abspath(os.path.join(script_dir, "../../../data/perso/bronze"))
silver = os.path.abspath(os.path.join(script_dir, "../../../data/perso/silver"))

os.makedirs(silver, exist_ok=True)

def nettoyer_espaces_verts():
    print("=== Nettoyage Espaces Verts ===")

    df = pd.read_csv(os.path.join(bronze, "espaces_verts.csv"), sep=";")

    # 1. Nettoyer le code postal
    df["Code postal"] = (
        df["Code postal"]
        .astype(str)
        .str.replace(".0", "", regex=False)  # supprime les ".0"
        .str.strip()
    )

    # 2. Filtrer uniquement les codes postaux de Paris (75001 → 75020)
    df = df[df["Code postal"].str.startswith("75")]

    # Supprimer les lignes vides, étranges, ou non numériques
    df = df[df["Code postal"].str.isnumeric()]

    # 3. Extraire l'arrondissement (ex: 75011 → 11)
    df["Arrondissement"] = df["Code postal"].astype(str).str[-2:].astype(int)

    # 4. Vérifier surface
    if "Surface calculée" not in df.columns:
        raise ValueError("Colonne 'Surface calculée' manquante !")

    df_clean = df[["Arrondissement", "Surface calculée"]]

    # 5. Agrégation
    agg = df_clean.groupby("Arrondissement")["Surface calculée"].sum().reset_index()
    agg.columns = ["Arrondissement", "Surface_espaces_verts_m2"]

    output_path = os.path.join(silver, "espaces_verts_silver.csv")
    agg.to_csv(output_path, index=False)

    print(f"[OK] Silver créé → {output_path}")


if __name__ == "__main__":
    nettoyer_espaces_verts()
