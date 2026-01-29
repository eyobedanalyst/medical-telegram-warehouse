import os
import json
import logging
from pathlib import Path
from datetime import date
import psycopg2
from psycopg2.extras import execute_batch, Json
from dotenv import load_dotenv

# --------------------------------------------------
# Load environment variables
# --------------------------------------------------

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

DATA_LAKE_DIR = Path("data/raw/telegram_messages")
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)

# --------------------------------------------------
# Logging
# --------------------------------------------------

logging.basicConfig(
    filename=LOGS_DIR / "load_raw.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

# --------------------------------------------------
# SQL
# --------------------------------------------------

CREATE_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;
"""

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw.telegram_messages (
    id BIGSERIAL PRIMARY KEY,
    message_id BIGINT,
    channel_name TEXT,
    message_date TIMESTAMPTZ,
    message_text TEXT,
    views INTEGER,
    forwards INTEGER,
    has_media BOOLEAN,
    image_path TEXT,
    scraped_date DATE,
    raw JSONB
);
"""

INSERT_SQL = """
INSERT INTO raw.telegram_messages (
    message_id,
    channel_name,
    message_date,
    message_text,
    views,
    forwards,
    has_media,
    image_path,
    scraped_date,
    raw
)
VALUES (
    %(message_id)s,
    %(channel_name)s,
    %(message_date)s,
    %(message_text)s,
    %(views)s,
    %(forwards)s,
    %(has_media)s,
    %(image_path)s,
    %(scraped_date)s,
    %(raw)s
);
"""

# --------------------------------------------------
# Load JSON files
# --------------------------------------------------

def read_data_lake() -> list[dict]:
    records = []

    for date_dir in DATA_LAKE_DIR.iterdir():
        if not date_dir.is_dir():
            continue

        scraped_date = date.fromisoformat(date_dir.name)

        for json_file in date_dir.glob("*.json"):
            channel_name = json_file.stem

            with open(json_file, "r", encoding="utf-8") as f:
                messages = json.load(f)

            for msg in messages:
                records.append({
                    "message_id": msg.get("message_id"),
                    "channel_name": channel_name,
                    "message_date": msg.get("date"),
                    "message_text": msg.get("text"),
                    "views": msg.get("views"),
                    "forwards": msg.get("forwards"),
                    "has_media": msg.get("has_media"),
                    "image_path": msg.get("image_path"),
                    "scraped_date": scraped_date,
                    "raw": Json(msg),
                })

    logger.info(f"Prepared {len(records)} records for loading")
    return records

# --------------------------------------------------
# Main
# --------------------------------------------------

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute(CREATE_SCHEMA_SQL)
        cur.execute(CREATE_TABLE_SQL)

    records = read_data_lake()

    if not records:
        logger.warning("No data found. Nothing to load.")
        return

    with conn.cursor() as cur:
        execute_batch(cur, INSERT_SQL, records, page_size=1000)

    conn.close()
    logger.info("Raw data successfully loaded into PostgreSQL")

if __name__ == "__main__":
    main()
