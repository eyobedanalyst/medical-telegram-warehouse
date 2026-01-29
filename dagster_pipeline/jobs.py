from dagster import job
from dagster_pipeline.ops import (
    scrape_telegram_data,
    load_raw_to_postgres,
    run_dbt_transformations,
    run_yolo_enrichment,
)

@job
def telegram_analytics_pipeline():
    scrape_telegram_data()
    load_raw_to_postgres()
    run_dbt_transformations()
    run_yolo_enrichment()
