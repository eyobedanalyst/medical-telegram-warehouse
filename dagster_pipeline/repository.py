from dagster import repository
from dagster_pipeline.jobs import telegram_analytics_pipeline

@repository
def medical_telegram_repo():
    return [telegram_analytics_pipeline]
