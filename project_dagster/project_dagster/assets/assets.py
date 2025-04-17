from dagster import asset, Output
import pandas as pd
import tempfile
import os
from kaggle.api.kaggle_api_extended import KaggleApi
import logging

logger = logging.getLogger(__name__)

@asset(
    required_resource_keys={"kaggle_credentials"},
    description="Télécharge les données ATP Tennis depuis Kaggle"
)
def atp_data_asset(context):
    """Télécharge les données ATP Tennis"""
    try:
        context.log.info("Vérification des credentials Kaggle")
        context.resources.kaggle_credentials
        
        api = KaggleApi()
        api.authenticate()
        context.log.info("Authentification Kaggle réussie")
        
        dataset = "dissfya/atp-tennis-2000-2023daily-pull"
        context.log.info(f"Accès au dataset: {dataset}")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            context.log.info(f"Téléchargement dans le répertoire temporaire: {temp_dir}")
            api.dataset_download_files(dataset, path=temp_dir, unzip=True)
            
            csv_file = None
            for file in os.listdir(temp_dir):
                if file.endswith('.csv'):
                    csv_file = os.path.join(temp_dir, file)
                    break
            
            if not csv_file:
                context.log.error("Aucun fichier CSV trouvé dans le dataset")
                raise ValueError("Aucun fichier CSV trouvé dans le dataset")
            
            context.log.info(f"Lecture du fichier: {csv_file}")
            df = pd.read_csv(csv_file)
            context.log.info(f"Données chargées. Dimensions: {df.shape}")
            
            return Output(df, metadata={
                "num_rows": len(df),
                "columns": list(df.columns),
                "sample": df.head(1).to_dict('records')[0]
            })

    except Exception as e:
        context.log.error(f"Erreur lors du téléchargement: {str(e)}", exc_info=True)
        raise