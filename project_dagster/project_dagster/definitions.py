from dagster import Definitions, load_assets_from_modules
from project_dagster import assets  # Assure-toi que 'assets' est bien importé
from project_dagster.resources import kaggle_credentials_resource 

# Charger tous les assets du module
all_assets = load_assets_from_modules([assets])

# Définir les ressources à utiliser
resource_defs = {
    "kaggle_credentials": kaggle_credentials_resource,
}

# Créer la configuration de pipeline
defs = Definitions(
    assets=all_assets,
    resources=resource_defs
)