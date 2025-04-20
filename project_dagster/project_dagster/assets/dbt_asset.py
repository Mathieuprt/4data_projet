import subprocess
from dagster import asset
from project_dagster.partitions import yearly_partitions

@asset(partitions_def=yearly_partitions)
def dbt_run(context):
    # Exécute DBT et enregistre les logs d'exécution
    try:
        context.log.info("Démarrage de l'exécution de DBT")
        
        result = subprocess.run(
            ['dbt', 'run'],
            capture_output=True,
            text=True,
            cwd='../projet_dbt' 
        )

        if result.returncode != 0:
            error_message = f"DBT run failed with error code {result.returncode}.\n"
            error_message += f"STDOUT:\n{result.stdout}\n"
            error_message += f"STDERR:\n{result.stderr}"
            context.log.error(f"Erreur DBT : {error_message}")
            raise Exception(error_message)
        
        context.log.info(f"Exécution DBT réussie. Résultat: {result.stdout}")
        return result.stdout
        
    except Exception as e:
        context.log.error(f"Erreur dans dbt_run : {str(e)}", exc_info=True)
        raise
