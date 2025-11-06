import os
import asyncio
import re
import csv
import logging
import time
from telethon import TelegramClient
from telethon.tl.types import User, Channel
from telethon.errors import FloodWaitError, ChannelPrivateError
from decouple import config
from typing import Dict, List, Optional


# --- Logging setup is unchanged ---
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
        self.csv_file = csv_file
        self.download_folder = download_folder
        self.pass_regex = r'```\s*([^\s`]+)\s*```'

        # CHANGED: Define the CSV headers in one place
        self.csv_fieldnames = [
            'channel_name', 'channel_id', 'sender_name', 'sender_id', 'date',
            'message_id', 'message', 'raw_text', 'file_name', 'file_size', 'pass_value'
        ]

        os.makedirs(self.download_folder, exist_ok=True)
        logging.info(f'Download folder set to {self.download_folder}')

        # CHANGED: Create the CSV and write headers if it doesn't exist
        self._init_csv()

    def _init_csv(self):
        """Creates the CSV file with headers if it doesn't exist."""
        try:
            # Check if file is empty
            file_exists = os.path.isfile(self.csv_file) and os.path.getsize(self.csv_file) > 0
            if not file_exists:
                with open(self.csv_file, mode='w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=self.csv_fieldnames)
                    writer.writeheader()
                logging.info(f"Initialized CSV file with headers: {self.csv_file}")
        except Exception as e:
            logging.error(f"Failed to initialize CSV file: {e}")
            print(f"ERROR: Failed to initialize CSV file: {e}")

    def _append_to_csv(self, message_data: Dict):
        """Appends a single message's data to the CSV file."""
        try:
            with open(self.csv_file, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.csv_fieldnames)
                writer.writerow(message_data)
            # Log to file
            logging.info(f"Appended message {message_data['message_id']} to CSV.")
            # Print to terminal
            print(f"Saved info for message {message_data['message_id']} to CSV.")
        except Exception as e:
            logging.error(f"Failed to append message {message_data['message_id']} to CSV: {e}")
            print(f"ERROR: Failed to save message {message_data['message_id']} to CSV: {e}")

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

    async def fetch_messages(self, limit: Optional[int] = 100) -> None: # CHANGED: No return value
        """Fetches, saves, and downloads messages one by one."""

        logging.info(f"Connecting to Telegram...")
        print(f"Connecting to Telegram...")

        async with self.telegram_client:
            try:
                entity = await self.telegram_client.get_entity(self.target_channel)
                logging.info(f"Found channel: {entity.title}")
                print(f"Found channel: {entity.title}")

                async for message in self.telegram_client.iter_messages(entity, limit=limit):
                    # 1. Parse the message data
                    parsed_data = self._parse_messages(
                        message=message, channel_name=entity.title, channel_id=entity.id
                    )

                    # 2. Append data to CSV *BEFORE* downloading
                    self._append_to_csv(parsed_data)

                    # 3. Download the file (if it exists)
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

                            print() # Newline after progress bar

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

        logging.info(f"Finished fetching messages.")
        print(f"Finished fetching messages.")
        # REMOVED: return messages_list

    # REMOVED: save_to_csv method is no longer needed

    def _get_progress_callback(self, file_name: str):
        """
        Returns a callback function to show download progress in the terminal and log file.
        (This is your advanced function, left unchanged)
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
        # CHANGED: No return value, no save_to_csv call
        await self.fetch_messages(limit=limit)


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

