from dagster import job
from project_dagster.assets.dbt_asset import dbt_run
from project_dagster.assets.atp_asset import atp_asset

@job
def atp_elt_job():
    atp_asset()
    dbt_run()