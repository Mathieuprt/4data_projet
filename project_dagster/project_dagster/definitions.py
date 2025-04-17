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

from dagster import Definitions
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project
from project_dagster import assets
from project_dagster.resources import kaggle_credentials_resource

DBT_PROJECT_DIR = os.path.join(os.path.dirname(__file__), "../projet_dbt")

dbt_resource = dbt_cli_resource.configured({
    "project_dir": DBT_PROJECT_DIR,
    "profiles_dir": DBT_PROJECT_DIR,
    "target": "dev" 
})

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
    key_prefix=["dbt"]  
)

defs = Definitions(
    assets=[
        *load_assets_from_modules([assets]), 
        *dbt_assets                        
    ],
    resources={
        "kaggle_credentials": kaggle_credentials_resource,
        "dbt": dbt_resource
    }
)