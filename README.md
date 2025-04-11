# Projet 4DATA

## Objectif du projet
Conception et déploiement d'une pipeline de données de bout en bout en utilisant Dagster comme orchestrateur. Cette pipemine suivre un processus ETL afin de récupérer, stocker et exploiter 

### Les données
Pour réaliser ce projet, nous faisons appel à deux datasets possédant les mêmes données mais pour deux circuits différents : ATP et WTA.
 ATP -> https://www.kaggle.com/datasets/dissfya/atp-tennis-2000-2023daily-pull
 WTA -> https://www.kaggle.com/datasets/dissfya/wta-tennis-2007-2023-daily-update


## Organisation du projet 
### Dagster
Pour lancer l'interface Dagster, se rendre dans le project_dagster (activer venv avant toute chose) et faire "dagster dev"



## Transformations à faire : 
1) Ne garder que les tournois Grand Chelem, Masters 1000, ATP 250 et 500. Equivalence sur le circuit féminin
2) Datas > 01/01/2015
3) Traduction des colonnes Court, Surface, Round
4) Vérification du pronostic
5) Création de colonnes personnalisés pour compter le nb de sets

