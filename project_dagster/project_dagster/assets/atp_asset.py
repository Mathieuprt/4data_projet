from dagster import asset, Output
import pandas as pd
import tempfile
import os
from kaggle.api.kaggle_api_extended import KaggleApi
import duckdb
from pathlib import Path

import logging
from project_dagster.partitions import yearly_partitions

logger = logging.getLogger(__name__)

@asset(
    name="atp_asset",
    required_resource_keys={"kaggle_credentials"},
    description="Télécharge et filtre les données ATP Tennis par année depuis Kaggle",
    partitions_def=yearly_partitions,
)
def atp_asset(context):
    """Télécharge les données ATP Tennis et les filtre selon la partition annuelle"""
    try:
        # Récupérer l'année de la partition
        if not hasattr(context, 'partition_key'):
            raise ValueError("Context doesn't have partition_key")
            
        year = context.partition_key
        context.log.info(f"Début du traitement pour l'année {year}")
        
        # Authentification Kaggle
        context.log.info("Vérification des credentials Kaggle")
        context.resources.kaggle_credentials
        
        api = KaggleApi()
        api.authenticate()
        context.log.info("Authentification Kaggle réussie")
        
        # Téléchargement des données
        dataset = "dissfya/atp-tennis-2000-2023daily-pull"
        context.log.info(f"Accès au dataset: {dataset}")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            context.log.info(f"Téléchargement dans le répertoire temporaire: {temp_dir}")
            api.dataset_download_files(dataset, path=temp_dir, unzip=True)
            
            csv_file = next(
                (os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.endswith('.csv'))
            )
            if not csv_file:
                raise ValueError("Aucun fichier CSV trouvé dans le dataset")
            
            # Chargement et filtrage des données
            context.log.info(f"Lecture et filtrage du fichier pour l'année {year}")
            df = pd.read_csv(csv_file)
            
            # Conversion de la colonne de date
            df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')

            # Filtrage par année de partition
            df_year = df[df['Date'].dt.year == int(year)].copy()
            
            # Optimisation mémoire
            # df_year = reduce_memory_usage(df_year)
            
            context.log.info(f"Données filtrées pour {year}. Dimensions: {df_year.shape}")

            # Connexion à la base DuckDB et écriture des données
            base_dir = Path(__file__).resolve().parents[3]  # monte jusqu'à "4DATA_PROJET"
            duckdb_path = base_dir / "projet_dbt" / "atp_tennis.duckdb"

            # Crée le dossier s'il n'existe pas
            duckdb_path.parent.mkdir(parents=True, exist_ok=True)
            context.log.info(f"Insertion des données dans la table DuckDB 'raw_matches' ({duckdb_path})")
            conn = duckdb.connect(str(duckdb_path))
            conn.register("df", df_year)
            conn.execute("""CREATE TABLE IF NOT EXISTS raw_matches AS SELECT * FROM df WHERE 1=0;""")
            conn.execute("""INSERT INTO raw_matches SELECT * FROM df""")
            conn.close()
            
             # Retour avec métadonnées enrichies
            context.log.info(f"Fin du traitement pour l'année {year}")
            return Output(
                df_year,
                metadata={
                    "num_rows": len(df_year),
                    "columns": list(df_year.columns),
                    "année": year,
                    "période": f"{df_year['Date'].min().date()} to {df_year['Date'].max().date()}",
                    "tournois": df_year['Tournament'].nunique()
                }
            )

    except Exception as e:
        error_year = context.partition_key if hasattr(context, 'partition_key') else 'inconnue'
        context.log.error(f"Erreur lors du traitement pour {error_year}: {str(e)}", exc_info=True)
        raise

def reduce_memory_usage(df):
    """Optimise l'utilisation mémoire du DataFrame"""
    for col in df.columns:
        if df[col].dtype == 'float64':
            df[col] = pd.to_numeric(df[col], downcast='float')
        elif df[col].dtype == 'int64':
            df[col] = pd.to_numeric(df[col], downcast='integer')
    return df
