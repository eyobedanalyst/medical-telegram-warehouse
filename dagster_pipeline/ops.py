from dagster import op, get_dagster_logger
import subprocess


@op
def scrape_telegram_data():
    logger = get_dagster_logger()
    logger.info("Starting Telegram scraping...")
    subprocess.run(["python", "scripts/scrape_telegram.py"], check=True)
    logger.info("Telegram scraping completed")


@op
def load_raw_to_postgres():
    logger = get_dagster_logger()
    logger.info("Loading raw data into Postgres...")
    subprocess.run(["python", "scripts/load_raw_to_postgres.py"], check=True)
    logger.info("Raw load completed")


@op
def run_dbt_transformations():
    logger = get_dagster_logger()
    logger.info("Running dbt transformations...")
    subprocess.run(["dbt", "run"], cwd="dbt", check=True)
    logger.info("dbt models completed")


@op
def run_yolo_enrichment():
    logger = get_dagster_logger()
    logger.info("Running YOLO image enrichment...")
    subprocess.run(["python", "scripts/run_yolo.py"], check=True)
    logger.info("YOLO enrichment completed")
