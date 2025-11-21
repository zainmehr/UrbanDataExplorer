import os
import pandas as pd

script_dir = os.path.dirname(os.path.abspath(__file__))
bronze = os.path.abspath(os.path.join(script_dir, "../../../data/perso/bronze"))
silver = os.path.abspath(os.path.join(script_dir, "../../../data/perso/silver"))

os.makedirs(silver, exist_ok=True)

def nettoyer_arbres():
    print("=== Nettoyage Arbres ===")

    # Charger le CSV brut
    df = pd.read_csv(os.path.join(bronze, "les-arbres.csv"), sep=";")

    # Extraire l'arrondissement sous forme de nombre (si présent)
    df["ArrNum"] = df["ARRONDISSEMENT"].str.extract(r"(\d+)")

    # Garder uniquement les lignes avec un arrondissement valide
    df = df[df["ArrNum"].notna()]      # enlever BOIS DE VINCENNES, etc.
    df["ArrNum"] = df["ArrNum"].astype(int)

    # Agréger : nombre d'arbres par arrondissement
    agg = df.groupby("ArrNum").size().reset_index(name="Nombre_arbres")
    agg.rename(columns={"ArrNum": "Arrondissement"}, inplace=True)

    # Export Silver
    output_path = os.path.join(silver, "arbres_silver.csv")
    agg.to_csv(output_path, index=False)

    print(f"[OK] Silver créé → {output_path}")

if __name__ == "__main__":
    nettoyer_arbres()
