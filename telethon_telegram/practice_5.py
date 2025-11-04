import os
import asyncio
import re
import polars as pl
import logging
import time
from telethon import TelegramClient
from telethon.tl.types import User, Channel
from telethon.errors import FloodWaitError, ChannelPrivateError
from decouple import config
from typing import Dict, List, Optional


file_handler = logging.FileHandler('scraper.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler]
)


class TelegramScraper:

    def __init__(self, api_id: int, api_hash: str, target_channel: str, telegram_session: str, download_folder: str = 'telegram_download', csv_file: str = 'telegram_messages.csv') -> None:
        self.telegram_client = TelegramClient(
            session=telegram_session,
            api_id=api_id,
            api_hash=api_hash,
        )
        self.target_channel = target_channel
        self.telegram_messages = []
        self.csv_file = csv_file
        self.download_folder = download_folder
        self.pass_regex = r'```\s*([^\s`]+)\s*```'

        os.makedirs(self.download_folder, exist_ok=True)
        logging.info(f'Download folder set to {self.download_folder}')

    def _parse_messages(self, message, channel_name, channel_id):
        """Helper function to parse a single message object into a dictionary."""
        pass_regex = re.search(self.pass_regex, message.text) if message.text else None

        sender_name: Optional[str] = None
        sender_id: Optional[int] = None
        if message.sender:
            if isinstance(message.sender, User):
                sender_name = message.sender.username
            elif isinstance(message.sender, Channel):
                sender_name = message.sender.title
            sender_id = message.sender.id

        return {
            'channel_name': channel_name,
            'channel_id': channel_id,
            'sender_name': sender_name,
            'sender_id': sender_id,
            'date': message.date.isoformat() if message.date else None,
            'message_id': message.id if message.id else None,
            'message': message.text,
            'raw_text': message.raw_text,
            'file_name': message.file.name if message.file else None,
            'file_size': message.file.size if message.file else None,
            'pass_value': pass_regex.group(1) if pass_regex else None,
        }

    async def fetch_messages(self, limit: Optional[int] = 100) -> List[Dict]:
        """Fetches messages and downloads files from the target channel."""

        messages_list: List[Dict] = []

        logging.info(f"Connecting to Telegram...")
        print(f"Connecting to Telegram...")

        async with self.telegram_client:
            try:
                entity = await self.telegram_client.get_entity(self.target_channel)
                logging.info(f"Found channel: {entity.title}")
                print(f"Found channel: {entity.title}")

                async for message in self.telegram_client.iter_messages(entity, limit=limit):
                    parsed_data = self._parse_messages(
                        message=message, channel_name=entity.title, channel_id=entity.id
                    )
                    messages_list.append(parsed_data)

                    if message.file:
                        file_name = message.file.name or f"file_{message.id}"
                        try:
                            file_path = os.path.join(self.download_folder, file_name)

                            if os.path.exists(file_path):
                                logging.info(f"File already exists, skipping: {file_path}")
                                print(f"File already exists, skipping: {file_name}")
                                continue

                            logging.info(f"Downloading file: {file_name}")

                            progress_func = self._get_progress_callback(file_name)

                            await message.download_media(
                                file=file_path,
                                progress_callback=progress_func,
                            )

                            print()

                            logging.info(f"Successfully downloaded: {file_name}")

                        except FloodWaitError as e:
                            logging.warning(f"Flood wait error. Sleeping for {e.seconds} seconds.")
                            print(f"Flood wait error. Sleeping for {e.seconds} seconds.")
                            await asyncio.sleep(e.seconds)
                        except Exception as e:
                            logging.error(f"Failed to download file {file_name}: {e}")
                            print(f"ERROR: Failed to download file {file_name}: {e}")

            except ChannelPrivateError:
                logging.error(f"Error: The channel '{self.target_channel}' is private or you don't have access.")
            except ValueError:
                logging.error(f"Error: Channel '{self.target_channel}' not found. Is the name correct?")
            except Exception as e:
                logging.error(f"An unexpected error occurred: {e}")

        logging.info(f"Finished fetching. Found {len(messages_list)} messages.")
        print(f"Finished fetching. Found {len(messages_list)} messages.") # Also print
        return messages_list


    def save_to_csv(self, messages: List[Dict]) -> None:
        """Saves the list of message dictionaries to a CSV file."""
        if not messages:
            logging.warning("No messages to save. Skipping CSV creation.")
            print("No messages to save. Skipping CSV creation.")
            return

        try:
            csv_directory = os.path.dirname(self.csv_file)
            if csv_directory:
                os.makedirs(csv_directory, exist_ok=True)

            df = pl.DataFrame(messages)
            df.write_csv(self.csv_file)
            logging.info(f"Successfully saved data to {self.csv_file}")
            print(f"Successfully saved data to {self.csv_file}")
        except Exception as e:
            logging.error(f"Failed to save data to CSV: {e}")
            print(f"ERROR: Failed to save data to CSV: {e}")

    def _get_progress_callback(self, file_name: str):
        """
        Returns a callback function to show download progress in the terminal and log file.
        """
        start_time = time.time()
        last_log_time = start_time
        last_logged_percent_bucket = -10  # so 0% will log

        def safe_div(a, b, default=0.0):
            try:
                return a / b if b else default
            except Exception:
                return default

        def progress_callback(received_bytes: int, total_bytes: int):
            nonlocal last_log_time, last_logged_percent_bucket
            now = time.time()
            elapsed_time = max(now - start_time, 1e-6)

            speed_mbps = (received_bytes / (1024 * 1024)) / elapsed_time
            received_mb = received_bytes / (1024 * 1024)
            total_mb = (total_bytes / (1024 * 1024)) if total_bytes else 0.0

            has_total = bool(total_bytes)
            percentage = safe_div(received_bytes * 100.0, total_bytes, 0.0) if has_total else 0.0

            remaining_bytes = (total_bytes - received_bytes) if has_total else None
            eta_seconds = (remaining_bytes / (speed_mbps * 1024 * 1024)) if (has_total and speed_mbps > 1e-9) else 0

            if has_total:
                progress_str = (
                    f"[{file_name[:20]}...] {percentage:6.1f}% "
                    f"({received_mb:6.1f} / {total_mb:.1f} MB) "
                    f"@ {speed_mbps:5.1f} MB/s "
                    f"ETA: {eta_seconds:4.0f}s"
                )
            else:
                progress_str = (
                    f"[{file_name[:20]}...] {received_mb:6.1f} MB "
                    f"@ {speed_mbps:5.1f} MB/s"
                )

            print(progress_str.ljust(80), end='\r', flush=True)

            # Throttled logging to file so speed is visible in scraper.log
            percent_bucket = int(percentage // 10) if has_total else None
            should_log_by_time = (now - last_log_time) >= 1.0
            should_log_by_percent = (has_total and percent_bucket is not None and percent_bucket > last_logged_percent_bucket)
            is_complete = has_total and received_bytes == total_bytes

            if should_log_by_time or should_log_by_percent or is_complete:
                logging.info(f"Downloading {file_name}: {progress_str.strip()}")
                last_log_time = now
                if has_total and percent_bucket is not None:
                    last_logged_percent_bucket = percent_bucket

            if has_total and received_bytes == total_bytes:
                # Print a newline so the last progress line doesn't overwrite subsequent prints
                print()

        return progress_callback



    async def run(self, limit: Optional[int] = 100):
        """Main execution flow."""
        messages = await self.fetch_messages(limit=limit)
        self.save_to_csv(messages)


if __name__ == '__main__':
    try:
        api_id = config('TELEGRAM_API_ID')
        api_hash = config('TELEGRAM_APP_API_HASH')
        telegram_session = config('TELEGRAM_SESSION', default='my_session')
        channel_name = config('CHANNEL_NAME')

        if not all([api_id, api_hash, channel_name]):
            logging.error("Missing required configuration (TELEGRAM_API_ID, TELEGRAM_APP_API_HASH, CHANNEL_NAME)")
            print("ERROR: Missing required configuration. Check scraper.log for details.")
        else:
            scraper = TelegramScraper(
                api_id=int(api_id),
                api_hash=api_hash,
                telegram_session=telegram_session,
                target_channel=channel_name,
                download_folder='telegram_downloads',
                csv_file='telegram_data.csv'
            )

            asyncio.run(scraper.run(limit=3))

    except KeyError as e:
        logging.error(f"Configuration error: Missing environment variable {e}")
        print(f"FATAL ERROR: Missing environment variable {e}. Check scraper.log.")
    except Exception as e:
        logging.error(f"An error occurred in main: {e}")
        print(f"FATAL ERROR: {e}. Check scraper.log.")
