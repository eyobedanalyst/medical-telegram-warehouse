from dagster import Definitions, ScheduleDefinition
from .pipeline import telegram_analytics_pipeline

daily_schedule = ScheduleDefinition(
    job=telegram_analytics_pipeline,
    cron_schedule="0 2 * * *",  # daily at 02:00
    execution_timezone="Africa/Addis_Ababa",
)

defs = Definitions(
    jobs=[telegram_analytics_pipeline],
    schedules=[daily_schedule],
)
