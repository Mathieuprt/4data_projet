from dagster import sensor, RunRequest, SkipReason, SensorEvaluationContext
from kaggle.api.kaggle_api_extended import KaggleApi
from datetime import datetime, timezone
import duckdb
from pathlib import Path
import os
from project_dagster.jobs import atp_elt_job

KAGGLE_DATASET = "dissfya/atp-tennis-2000-2023daily-pull"

def parse_kaggle_date(date_input):
    """Parse Kaggle date which can be either string or datetime"""
    if isinstance(date_input, datetime):
        return date_input.replace(tzinfo=timezone.utc)
    try:
        return datetime.strptime(date_input, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.fromisoformat(date_input).astimezone(timezone.utc)

@sensor(job=atp_elt_job)
def new_tournament_sensor(context: SensorEvaluationContext):
    try:
        # Vérification des identifiants Kaggle
        kaggle_config_path = Path.home() / ".kaggle" / "kaggle.json"
        if not kaggle_config_path.exists():
            return SkipReason("Kaggle credentials not found at ~/.kaggle/kaggle.json")
        
        os.chmod(kaggle_config_path, 0o600)

        api = KaggleApi()
        api.authenticate()
        context.log.info("Kaggle authentication successful")

        # Récupération de la date de dernière mise à jour
        try:
            dataset = api.dataset_metadata(KAGGLE_DATASET)
            last_updated = dataset.lastUpdated if hasattr(dataset, 'lastUpdated') else dataset.last_updated
        except Exception:
            datasets = api.dataset_list(search=KAGGLE_DATASET.split('/')[1])
            if not datasets:
                return SkipReason("Dataset not found on Kaggle")
            dataset = next((d for d in datasets if d.ref == KAGGLE_DATASET), None)
            if not dataset:
                return SkipReason("Dataset not found in search results")
            last_updated = dataset.lastUpdated if hasattr(dataset, 'lastUpdated') else dataset.last_updated

        kaggle_last_updated_dt = parse_kaggle_date(last_updated)

        # Connexion DuckDB
        base_dir = Path(__file__).resolve().parents[3]
        duckdb_path = base_dir / "projet_dbt" / "atp_tennis.duckdb"

        if not duckdb_path.exists():
            context.log.info("DuckDB database not found. Execution needed.")
            current_year = str(datetime.now().year)
            return RunRequest(
                run_key=f"kaggle_update_{kaggle_last_updated_dt.timestamp()}",
                partition_key=current_year
            )

        conn = duckdb.connect(str(duckdb_path))
        result = conn.execute("SELECT MAX(Date) FROM raw_matches").fetchone()
        conn.close()

        if not result or not result[0]:
            context.log.info("No data in raw_matches. Execution needed.")
            current_year = str(datetime.now().year)
            return RunRequest(
                run_key=f"kaggle_update_{kaggle_last_updated_dt.timestamp()}",
                partition_key=current_year
            )

        last_date_in_db = result[0]
        if isinstance(last_date_in_db, str):
            last_date_in_db = datetime.strptime(last_date_in_db, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        elif not hasattr(last_date_in_db, 'tzinfo'):
            last_date_in_db = last_date_in_db.replace(tzinfo=timezone.utc)

        if kaggle_last_updated_dt > last_date_in_db:
            context.log.info(f"New data detected on Kaggle (update: {kaggle_last_updated_dt}, db: {last_date_in_db})")
            update_year = str(kaggle_last_updated_dt.year)
            return RunRequest(
                run_key=f"kaggle_update_{kaggle_last_updated_dt.timestamp()}",
                partition_key=update_year
            )

        return SkipReason("Pas de nouvelles données sur Kaggle")

    except Exception as e:
        context.log.error(f"Sensor error: {str(e)}", exc_info=True)
        return SkipReason(f"Detection error: {str(e)}")