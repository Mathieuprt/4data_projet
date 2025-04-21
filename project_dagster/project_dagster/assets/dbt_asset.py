import subprocess
from dagster import asset
from project_dagster.partitions import yearly_partitions

@asset(partitions_def=yearly_partitions)
def dbt_run(atp_asset):
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
        raise Exception(error_message)
    
    return result.stdout
