# Projet 4Data

## Objectif du projet

Ce projet implémente une pipeline de données complète autour des matchs ATP Tennis (2000–2023) à l'aide de **Dagster** pour l'orchestration, **DBT** pour la transformation, et **DuckDB** comme moteur de stockage. L'objectif est de :
- Collecter des données depuis Kaggle (dataset ATP)
- Les filtrer et charger par année (pipeline partitionné)
- Les transformer via des modèles DBT
- Exporter les résultats pour des outils d'analyse comme Power BI

## Source de données 

https://www.kaggle.com/datasets/dissfya/atp-tennis-2000-2023daily-pull

## Choix de conception

### Pipeline partitionné par année
Chaque exécution traite les données d’une **année spécifique**, facilitant la réexécution, la maintenance et l’analyse temporelle.

### Stockage dans DuckDB
Nous utilisons DuckDB pour sa légèreté, sa compatibilité avec DBT. Néanmoins, Power BI n'intégrant pas de connecteurs natifs avec DuckDB, un script Python a été mis en place pour exporter les données en csv

### Outils utilisés
- **Dagster** : orchestration des assets et jobs
- **DBT** : transformation SQL modulaire et testable
- **DuckDB** : base de données fichier performante
- **Power BI** : visualisation finale (via fichiers `.csv` ou ODBC)


## Étapes d'installation

### Prérequis

- Python 3.9+
- `pip` ou `venv`
- Compte Kaggle avec API key (`kaggle.json`)

### Installation

```bash
cd 4data_projet
python -m venv venv
venv\Scripts\activate        # ou source venv/bin/activate sur Mac/Linux
cd ../..
pip install -r requirements.txt
```

### Configuration

Placer le fichier `kaggle.json` dans :
```
C:\Users\<user>\.kaggle
```

---

## Exécution de la pipeline

### 1. Lancer Dagster

```bash
dagster dev -f project_dagster/project_dagster
```

### 2. Depuis l'interface Dagster

- Aller dans **Jobs > atp_elt_job**
- Cliquer sur **Launch Partition Backfill**
- Choisir une année, puis lancer

### 3. En ligne de commande

```bash
dagster job launch -j atp_elt_job -p 2023
```

### 4. Export CSV (optionnel)

```bash
python project_dagster/scripts/export_all_tables.py
```

Puis connecter les fichiers `.csv` à Power BI.

---

## 📈 Monitoring & Logs

- Suivi en direct via Dagster UI (`Runs`, `Assets`, `Logs`)

---

## Planification automatique

La pipeline est planifiée pour s’exécuter **tous les jours à 18h (Europe/Paris)** via `build_schedule_from_partitioned_job`.

---

## Tests

Lancer les tests unitaires avec :

```bash
pytest
```

Les tests se trouvent dans :
```
project_dagster/project_dagster_tests/
```
