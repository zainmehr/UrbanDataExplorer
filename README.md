**README -- Branche perso**
==========================

✔ Indicateurs choisis
---------------------

J'ai ajouté **2 indicateurs perso** dans le projet :

-   **Espaces verts** (surface totale par arrondissement)

-   **Arbres** (nombre d'arbres par arrondissement)

Ces indicateurs sont différents de ceux du groupe.

* * * * *

✔ Pipeline perso (simple)
-------------------------

Mes fichiers sont séparés dans :

`data/perso/
scripts/perso/`

-   **Bronze** → CSV bruts

-   **Silver** → nettoyage + agrégations

-   **Gold** → fusion finale des 2 indicateurs

* * * * *

✔ Sortie finale (Gold)
----------------------

Le fichier final généré est :

`data/perso/gold/indicateurs_perso_gold.csv`

Il contient, pour chaque arrondissement de Paris :

-   la surface d'espaces verts

-   le nombre d'arbres