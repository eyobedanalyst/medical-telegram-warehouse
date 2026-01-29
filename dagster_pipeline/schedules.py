from dagster import schedule
from dagster_pipeline.jobs import telegram_analytics_pipeline

@schedule(
    cron_schedule="0 2 * * *",
    job=telegram_analytics_pipeline,
    execution_timezone="Africa/Addis_Ababa",
)
def daily_pipeline_schedule():
    return {}
