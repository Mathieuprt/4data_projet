from dagster import job, define_asset_job
from project_dagster.assets.dbt_asset import dbt_run
from project_dagster.assets.atp_asset import atp_asset
from project_dagster.partitions import yearly_partitions

# @job
# def atp_elt_job():
#     atp_asset()
#     dbt_run()

# atp_elt_job = define_asset_job(
#     name="atp_elt_job",
#     partitions_def=yearly_partitions
# )

# Job basé sur les assets partitionnés (auto-détection des dépendances)
atp_elt_job = define_asset_job(
    name="atp_elt_job",
    partitions_def=yearly_partitions
)