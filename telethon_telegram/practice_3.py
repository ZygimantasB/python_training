import os
import asyncio
import re
import polars as pl
import logging
import crypt

from decouple import config
from telethon import TelegramClient, events
from pprint import pprint

logger = logging.getLogger(__name__)

api_id = config('TELEGRAM_API_ID')
api_hash = config('TELEGRAM_APP_API_HASH')

client = TelegramClient('telegram_session_tarnybinis', api_id=api_id, api_hash=api_hash)

channels_count = []
download_folder = r'telegram_download'

target_channel = config('CHANNEL_NAME')
messages_df = []

pass_regex = r'```\s*([^\s`]+)\s*```'

@events.register(events.NewMessage('hello'))
async def main():

    channel_index = 1

    async with client:
        me = await client.get_me()

        logger.info(f'Successfully connected as {me.first_name} {me.last_name}')

        async for dialog in client.iter_dialogs():

            chanel_name = dialog.name
            channel_id = dialog.id
            logger.info(f'Successfully connected to channel name {chanel_name} channel ID {channel_id}')
            # channels_count.append({
            #     'channel': channel_index,
            #     'name': chanel_name,
            # })

            logger.info(f'Started logging massages')
            if dialog.title == target_channel:
                async for message in client.iter_messages(dialog.entity, limit=1_000):
                    pass_match = re.search(pass_regex, message.text) if message.text else None

                    messages_df.append({
                        'chanel_name': chanel_name,
                        'channel_id': channel_id,
                        'sender': message.sender.username if message.sender else None,
                        'sender s': message.sender.usernames if message.sender else None,
                        'messages': message.text,
                        'pass_match': pass_match.group(1) if pass_match else None,
                        'date': message.date.isoformat(),
                        'file': message.file.size if message.file else None,
                        'file_name': message.file.name if message.file else None,
                    })

        print(pl.DataFrame(messages_df).write_csv('telegram_download/telegram_messages_all.csv'))

        # pl.DataFrame(messages_df).write_csv(download_folder + '/telegram_messages_test.csv')


if __name__ == '__main__':
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s - %(module)s - %(pathname)s - %(thread)d - %(threadName)s -%(taskName)s')
    file_handler = logging.FileHandler('telegram_download/telegram_logs.log')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(log_formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(log_formatter)

    root_logger = logging.getLogger()
    root_logger.addHandler(file_handler)
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)

    logging.getLogger('telethon').setLevel(logging.WARNING)

    asyncio.run(main())
