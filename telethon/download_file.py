import asyncio
import logging
import os
import re
from pathlib import Path

from decouple import config
from telethon import TelegramClient
from telethon.errors.rpcerrorlist import FloodWaitError

# Basic logging (WARNING by default, override with TELEGRAM_LOG_LEVEL if desired)
log_level_name = os.getenv("TELEGRAM_LOG_LEVEL", "WARNING").upper()
log_level = getattr(logging, log_level_name, logging.WARNING)
logging.basicConfig(
    format='[%(levelname)s %(asctime)s] %(name)s: %(message)s',
    level=log_level
)
logger = logging.getLogger("downloader")

# Credentials from .env via python-decouple
api_id = config("telegram_api_id", cast=int)
api_hash = config("telegram_api_hash")

# Optional overrides
session_name = os.getenv("TELEGRAM_SESSION", "session_name")
base_download_dir = Path(os.getenv("TELEGRAM_DOWNLOAD_DIR", "telegram_download")).resolve()
# If true, will also process regular small group chats; by default we stick to channels (incl. megagroups)
include_groups = os.getenv("TELEGRAM_INCLUDE_GROUPS", "0").strip() in {"1", "true", "yes", "on"}

client = TelegramClient(session_name, api_id, api_hash)


def _safe_name(name: str) -> str:
    """Make a filesystem-safe folder name from a dialog title."""
    # Remove/replace characters that are problematic on Windows paths
    name = re.sub(r"[\\/:*?\"<>|]", "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name or "unnamed"


async def _load_downloaded_set(folder: Path) -> set[int]:
    """Read the set of already-downloaded message IDs for this folder."""
    idx_file = folder / ".downloaded_ids.txt"
    if not idx_file.exists():
        return set()
    try:
        with idx_file.open("r", encoding="utf-8") as f:
            return {int(line.strip()) for line in f if line.strip().isdigit()}
    except Exception as e:
        logger.warning("Failed to read index file %s: %s", idx_file, e)
        return set()


def _append_downloaded_id(folder: Path, mid: int) -> None:
    idx_file = folder / ".downloaded_ids.txt"
    with idx_file.open("a", encoding="utf-8") as f:
        f.write(f"{mid}\n")


async def download_from_dialog(entity, dialog_name: str) -> None:
    channel_dir = base_download_dir / _safe_name(dialog_name)
    channel_dir.mkdir(parents=True, exist_ok=True)

    downloaded = await _load_downloaded_set(channel_dir)

    async def progress(current: int, total: int):
        if total:
            pct = current / total * 100
            print(f"[{dialog_name}] Downloaded {current}/{total} bytes ({pct:.2f}%)", end="\r")

    logger.info("Scanning messages in '%s'...", dialog_name)

    async for message in client.iter_messages(entity):
        # Only messages that actually contain downloadable files/media
        if not message.file:
            continue
        if message.id in downloaded:
            continue
        try:
            path = await client.download_media(
                message,
                file=str(channel_dir),
                progress_callback=progress,
            )
            if path:
                print()  # newline after progress\r
                logger.info("[%s] Saved: %s (message id=%s)", dialog_name, path, message.id)
                _append_downloaded_id(channel_dir, message.id)
        except FloodWaitError as fw:
            # Respect Telegram rate limits
            wait = int(getattr(fw, "seconds", 5))
            logger.warning("Flood wait for %s seconds while downloading from '%s'", wait, dialog_name)
            await asyncio.sleep(wait)
        except Exception as e:
            logger.exception("Failed to download media from '%s' message id=%s: %s", dialog_name, message.id, e)


async def main():
    base_download_dir.mkdir(parents=True, exist_ok=True)

    # Using async context to ensure proper connection lifecycle
    async with client:
        logger.info("Fetching your dialogs (chats/channels)...")
        async for dialog in client.iter_dialogs():
            # dialog.is_channel includes broadcast channels and megagroups
            is_channel = getattr(dialog, "is_channel", False)
            is_group = getattr(dialog, "is_group", False)

            if is_channel or (include_groups and is_group):
                name = dialog.name or str(dialog.id)
                try:
                    await download_from_dialog(dialog.entity, name)
                except Exception as e:
                    logger.exception("Error processing '%s': %s", name, e)


if __name__ == "__main__":
    asyncio.run(main())
