from dagster import ScheduleDefinition, build_schedule_from_partitioned_job
from project_dagster.jobs import atp_elt_job

# daily_schedule = ScheduleDefinition(
#     job=atp_elt_job,
#     cron_schedule="0 18 * * *",
#     execution_timezone="Europe/Paris"
# )

# Crée un schedule basé sur la dernière partition disponible
daily_schedule = build_schedule_from_partitioned_job(
    job=atp_elt_job,
    cron_schedule="0 18 * * *",  # tous les jours à 18h
    execution_timezone="Europe/Paris",
    name="daily_atp_schedule"
)