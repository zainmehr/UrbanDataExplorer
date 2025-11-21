# UrbanDataExplorer
Pour l’intégration de ma partie API (transactions.py)
Comme la MAJ de l’API était pas encore push sur le git, j’ai continué mes endpoints de mon côté. J’ai tout mis dans un fichier dédié, transactions.py, histoire d’éviter les embrouilles au moment du merge et garder un truc propre.

J’ai tout fait avec un APIRouter, donc c’est littéralement plug-and-play.

Ce qu’il faudra faire pour l’intégrer

C’est rapide, y’a juste deux trucs à ajouter dans le main.py quand vous l’aurez :

Installez les libs si c’est pas déjà fait :
pip install fastapi uvicorn mysql-connector-python

Dans le main.py, mettez ça :

# Import du router
from transactions import router as transactions_router

# Ajout du router dans l'app
app.include_router(transactions_router)

⚠️ Important : la config BDD

J’ai mis une config par défaut dans transactions.py pour bosser en local.
Checkez bien la variable db_config en haut du fichier pour voir si ça correspond à nos infos de groupe (user/password/nom de la base). Sinon ça va crash direct à la connexion.

Endpoints dispo

Une fois merge, tout passera par /transactions :

/transactions/types : liste des types de transactions

/transactions/spending/{user_id} : dépenses par user

/transactions/timeline : l’évolution dans le temps (pratique pour les graphs)

Si vous voulez tester mon code sans lancer tout le projet, j’ai un petit script de test. Hésitez pas.