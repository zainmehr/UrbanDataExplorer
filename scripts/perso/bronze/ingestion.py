import os

# Dossiers
script_dir = os.path.dirname(os.path.abspath(__file__))
bronze_dir = os.path.abspath(os.path.join(script_dir, "../../../data/perso/bronze"))

required_files = [
    "espaces_verts.csv",
    "les-arbres.csv",
]

def verify_bronze():
    print("=== Vérification des fichiers Bronze perso ===\n")

    for file in required_files:
        path = os.path.join(bronze_dir, file)
        if os.path.exists(path):
            print(f"[OK] {file} trouvé")
        else:
            print(f"[ERREUR] {file} manquant !  → doit être placé dans data/perso/bronze/")

if __name__ == "__main__":
    verify_bronze()
