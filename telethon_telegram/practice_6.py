import os
import re
import logging
import asyncio
import sqlite3

from telethon import TelegramClient
from decouple import config
from typing import Dict, List, Optional

os.makedirs('logs', exist_ok=True)

log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler('logs/scraper.log')
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_formatter)
logger.addHandler(stream_handler)

class TelegramScraper:

    def __init__(self, api_id: int, api_hash: str, session_name: str, target_channel: str, download_folder: str = 'telegram_downloads'):

        self.client = TelegramClient(session_name, api_id, api_hash)
        self.target_channel = target_channel
        self.download_folder = download_folder
        self.pass_regex = r'```\s*([^\s`]+)\s*```'
        self.messages = []
        self.entity = None

    def _sanitize_filename(self, name: str) -> str:
        if not name:
            return 'unknown'
        return re.sub(r'[\\/*?:"<>|]', "_", name)

    def _parse_messages(self, message, channel_name: str, channel_id: str) -> Dict[str, str]:
        match = re.search(self.pass_regex, message.text) if message.text else None

        pass_match_text = match.group(1) if match else None

        return {
            'channel_name': channel_name,
            'channel_id': channel_id,
            'message_id': message.id,
            'sender': message.sender.username if message.sender else None,
            'message_text': message.text,
            'message_raw_text': message.raw_text,
            'message_date': message.date.isoformat() if message.date else None,
            'file_name': message.file.name if message.file else None,
            'file_size': message.file.size if message.file else None,
            'pass_match': pass_match_text
        }

    async def _fetch_messages(self, limit=10):
        try:
            self.entity = await self.client.get_entity(self.target_channel)
            logging.info(f"Fetching messages from {self.target_channel}")
        except ValueError:
            logging.info(f'Channel not found: {self.target_channel}')
            return
        except Exception as e:
            logging.error(f'Error while fetching messages: {self.target_channel} {e}')
        async for message in  self.client.iter_messages(self.target_channel, limit=limit):
            logging.info(f'Message id found {message.id}')
            self.messages.append(message)

    def save_to_sqlite(self, db_name: str = 'telegra_messages.db', table_name: str = 'messages'):

        if not self.messages:
            logging.info(f"No messages found for channel: {self.target_channel}")
            return

        if not self.entity:
            logging.error(f'No entity found for channel: {self.target_channel}')
            return

        parsed_messages = []
        for msg in self.messages:
            parse_data = self._parse_messages(msg, self.entity.title, self.entity.id)
            parsed_messages.append(parse_data)

        db_path = os.path.join(self.download_folder, db_name)
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        conn = None
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            cursor.execute(f"""
                            CREATE TABLE IF NOT EXISTS {table_name} (
                                CHANNEL_NAME TEXT,
                                CHANNEL_ID TEXT,
                                MESSAGE_ID INTEGER,
                                SENDER TEXT,
                                MESSAGE_TEXT TEXT,
                                MESSAGE_RAW_TEXT TEXT,
                                MESSAGE_DATE TEXT,
                                FILE_NAME TEXT,
                                FILE_SIZE INTEGER,
                                PASS_MATCH TEXT
                            )
                        """)

            cols = parsed_messages[0].keys()
            placeholders = ", ".join(["?"] * len(cols))
            data_to_insert = [tuple(msg[col] for col in cols) for msg in parsed_messages]

            insert_query = f"INSERT OR IGNORE INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})"


            cursor.executemany(insert_query, data_to_insert)
            conn.commit()
            logging.info(f'Successfully saved messages to {db_path}')

        except sqlite3.Error as e:
            logging.error(f"Error while saving messages: {e}")
        finally:
            if conn:
                conn.close()

    async def _download_files(self):
        if not self.messages:
            logging.info(f"No messages found for channel: {self.target_channel}")
            return
        if not self.entity:
            logging.error(f'No entity found for channel: {self.target_channel}')

        logging.info(f"Starting download process for {len(self.messages)} messages...")

        safe_channel_name = self._sanitize_filename(self.entity.title)
        channel_download_path = os.path.join(self.download_folder, safe_channel_name)
        os.makedirs(channel_download_path, exist_ok=True)
        logging.info(f"Saving files to {channel_download_path}")

        for message in self.messages:
            if message.media:
                try:
                    file_name = message.file.name if message.file and message.file.name else f"media_from_message_{message.id}"
                    logging.info(f'Attempting to download file {file_name}')

                    downloaded_path = await message.download_media(file=channel_download_path)

                    logging.info(f'Successfully downloaded file {file_name}')
                except Exception as e:
                    logging.error(f'Failed to download the file from message {message.id} {e}')


    async def run(self):
        async with self.client:
            await self._fetch_messages(limit=20)
            self.save_to_sqlite(db_name='telegram_messages.db', table_name='messages')
            await self._download_files()

if '__main__' == __name__:

    api_id = config('TELEGRAM_API_ID')
    api_hash = config('TELEGRAM_APP_API_HASH')
    session_name = config('TELEGRAM_SESSION')
    target_channel = config('CHANNEL_NAME')

    scraper = TelegramScraper(
        api_id=api_id,
        api_hash=api_hash,
        session_name=session_name,
        target_channel=target_channel,
        download_folder='telegram_downloads'
    )

    asyncio.run(scraper.run())
