from dagster import job
from .ops import (
    scrape_telegram_data,
    load_raw_to_postgres,
    run_dbt_transformations,
    run_yolo_enrichment,
)


@job(description="End-to-end Medical Telegram Analytics Pipeline")
def telegram_analytics_pipeline():
    scraped = scrape_telegram_data()
    loaded = load_raw_to_postgres(scraped)
    transformed = run_dbt_transformations(loaded)
    run_yolo_enrichment(transformed)
