import os
import json
import logging
from datetime import datetime,timezone
from pathlib import Path
import asyncio
from telethon import TelegramClient
from telethon.tl.types import Message, Channel
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.errors import FloodWaitError
from dotenv import load_dotenv
from telethon.tl.types import MessageMediaPhoto
# -----------------------------------------------------
# Environment & configuration
# -----------------------------------------------------

load_dotenv()

API_ID = int(os.getenv("TELEGRAM_API_ID"))
API_HASH = os.getenv("TELEGRAM_API_HASH")

BASE_DATA_DIR = Path("data/raw")
MESSAGES_DIR = BASE_DATA_DIR / "telegram_messages"
IMAGES_DIR = BASE_DATA_DIR / "images"
LOGS_DIR = Path("logs")

LOGS_DIR.mkdir(exist_ok=True)

# -----------------------------------------------------
# Logging configuration
# -----------------------------------------------------

logging.basicConfig(
    filename=LOGS_DIR / "telegram_scraper.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

# -----------------------------------------------------
# Telegram channels to scrape
# -----------------------------------------------------

TARGET_CHANNELS = [
    "@tikvahpharma",
    "@lobelia4cosmetics",
    "@tenamereja",
]

# -----------------------------------------------------
# Scraping logic
# -----------------------------------------------------

async def scrape_channel(client: TelegramClient, channel_handle: str):
    logger.info(f"Resolving channel: {channel_handle}")

    try:
        entity = await client.get_entity(channel_handle)
    except Exception as e:
        logger.error(f"Cannot resolve {channel_handle}: {e}")
        return

    if not isinstance(entity, Channel):
        logger.error(f"{channel_handle} is not a channel")
        return

    channel_name = entity.username or str(entity.id)

    # Join channel if required
    try:
        await client(JoinChannelRequest(entity))
        logger.info(f"Joined channel {channel_name}")
    except Exception:
        pass

    messages = []

    async for message in client.iter_messages(entity, limit=200):
        image_path = None
        has_media = message.media is not None

        if has_media and isinstance(message.media, MessageMediaPhoto):
            channel_dir = IMAGES_DIR / channel_name
            channel_dir.mkdir(parents=True, exist_ok=True)
            image_path = channel_dir / f"{message.id}.jpg"
            try:
                await client.download_media(message.media, file=image_path)
            except Exception as e:
                logger.warning(f"Failed to download image for message {message.id}: {e}")
                image_path = None

        messages.append({
            "message_id": message.id,
            "channel_name": channel_name,
            "date": message.date.isoformat() if message.date else None,
            "text": message.text,
            "views": message.views,
            "forwards": message.forwards,
            "has_media": has_media,
            "image_path": str(image_path) if image_path else None,
        })




    if not messages:
        logger.warning(f"No messages found in {channel_name}")
        return

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_dir = MESSAGES_DIR / date_str
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{channel_name}.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(messages, f, indent=2, ensure_ascii=False)

    logger.info(f"Saved {len(messages)} messages from {channel_name}")

# -----------------------------------------------------
# Entrypoint
# -----------------------------------------------------

async def main():
    async with TelegramClient("telegram_session", API_ID, API_HASH) as client:
        for channel in TARGET_CHANNELS:
            try:
                await scrape_channel(client, channel)
            except FloodWaitError as e:
                logger.error(f"Rate limited for {e.seconds} seconds")
            except Exception as e:
                logger.exception(f"Unhandled error while scraping {channel}: {e}")

if __name__ == "__main__":
    asyncio.run(main())
