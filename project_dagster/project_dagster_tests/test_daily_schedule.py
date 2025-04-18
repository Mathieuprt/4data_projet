from project_dagster.schedules import daily_schedule

def test_daily_schedule():
    assert daily_schedule.cron_schedule == "0 6 * * *"
    assert daily_schedule.execution_timezone == "Europe/Paris"
    assert daily_schedule.job.name == "atp_elt_job"
