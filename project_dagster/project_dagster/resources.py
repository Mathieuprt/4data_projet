from dagster import resource
import os
import json
import logging

logger = logging.getLogger(__name__)

@resource
def kaggle_credentials_resource(context):
    # Authentifie avec Kaggle en utilisant kaggle.json
    try:
        kaggle_path = r"config/kaggle.json"
        context.log.info(f"Recherche du fichier kaggle.json à: {kaggle_path}")
        
        if not os.path.exists(kaggle_path):
            raise FileNotFoundError(f"Fichier kaggle.json introuvable à l'emplacement: {kaggle_path}")
        
        with open(kaggle_path, encoding='utf-8') as f:
            credentials = json.load(f)
        
        os.environ['KAGGLE_USERNAME'] = credentials['username']
        os.environ['KAGGLE_KEY'] = credentials['key']
        
        context.log.info("Authentification Kaggle configurée avec succès")
        return True
        
    except Exception as e:
        context.log.error(f"Erreur d'authentification: {str(e)}", exc_info=True)
        raise