import subprocess
from dagster import asset

@asset
def dbt_run():
    result = subprocess.run(
        ['dbt', 'run'],
        capture_output=True,
        text=True,
        cwd='../projet_dbt'  # Spécifie le chemin correct vers ton répertoire dbt_project.yml
    )

    # Si l'exécution échoue, affiche la sortie d'erreur (stderr)
    if result.returncode != 0:
        error_message = f"DBT run failed with error code {result.returncode}.\n"
        error_message += f"STDOUT:\n{result.stdout}\n"
        error_message += f"STDERR:\n{result.stderr}"
        raise Exception(error_message)
    
    return result.stdout
