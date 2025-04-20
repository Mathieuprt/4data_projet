from dagster import ScheduleDefinition
from project_dagster.jobs import atp_elt_job

daily_schedule = ScheduleDefinition(
    job=atp_elt_job,
    cron_schedule="0 18 * * *",
    execution_timezone="Europe/Paris"
)