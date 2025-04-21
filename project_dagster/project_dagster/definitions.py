from dagster import Definitions, load_assets_from_modules
from project_dagster.assets import atp_asset, dbt_asset
from project_dagster.resources import kaggle_credentials_resource 
from project_dagster.jobs import atp_elt_job
from project_dagster.schedules import daily_schedule
from project_dagster.sensors import new_tournament_sensor
from project_dagster.loggers import file_logger

# Chargement des assets
all_assets = load_assets_from_modules(
    modules=[atp_asset, dbt_asset],
    group_name="atp_tennis"
)

# Configuration des ressources
resource_defs = {
    "kaggle_credentials": kaggle_credentials_resource,
}

defs = Definitions(
    assets=all_assets,
    resources=resource_defs,
    jobs=[atp_elt_job],
    schedules=[daily_schedule],
    sensors=[new_tournament_sensor],
    loggers={"file": file_logger},  # Ajout du logger personnalisé
)