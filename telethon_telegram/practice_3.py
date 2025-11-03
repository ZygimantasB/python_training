import os
import asyncio
import re
import polars as pl

from decouple import config
from telethon import TelegramClient, events
from pprint import pprint

api_id = config('telegram_api_id')
api_hash = config('telegram_app_api_hash')

client = TelegramClient('telegram_session_tarnybinis', api_id=api_id, api_hash=api_hash)

channels_count = []
download_folder = r'telegram_download'

target_channel = config('channel_name')
messages_df = []

pass_regex = r'```\s*([^\s`]+)\s*```'

@events.register(events.NewMessage('hello')
async def main():

    channel_index = 1

    async with client:
        me = await client.get_me()
        print(me.first_name)
        print(me.last_name)


        async for dialog in client.iter_dialogs():

            chanel_name = dialog.name
            channel_id = dialog.id

            # channels_count.append({
            #     'channel': channel_index,
            #     'name': chanel_name,
            # })


            if dialog.title == target_channel:
                async for message in client.iter_messages(dialog.entity, limit=10):

                    pass_match = re.search(pass_regex, message.text) if message.text else None

                    messages_df.append({
                        'chanel_name': chanel_name,
                        'channel_id': channel_id,
                        'sender': message.sender.username if message.sender else None,
                        'sender s': message.sender.usernames if message.sender else None,
                        'messages': message.text,
                        'message_raw': message.raw_text,
                        'pass_match': pass_match.group(1) if pass_match else None,
                        'date': message.date.isoformat(),
                        'sender_date': message.date.isoformat(),
                        'file': message.file.size if message.file else None,
                        'file_name': message.file.name if message.file else None,
                    })

                    pprint(message.sender.to_dict())

        print(pl.DataFrame(messages_df))

        # pl.DataFrame(messages_df).write_csv(download_folder + '/telegram_messages_test.csv')


if __name__ == '__main__':
    asyncio.run(main())
