import asyncio
import re
import polars as pl
import os

from pprint import pprint
from telethon import TelegramClient, events
from decouple import config

api_id = config("TELEGRAM_API_ID")
api_hash = config("TELEGRAM_APP_API_HASH")

client = TelegramClient('telegram_session_tarnybinis', api_id=api_id, api_hash=api_hash)

def sanitize_filename(name):
    name = re.sub(r'[<>:"/\\|?*]', '', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip()

async def main():

    async with client:
        me = await client.get_me()
        # print(me.stringify())

        username = me.username
        # print(me.username)
        # print(me.id)
        # print(me.phone)
        # print(me.first_name)
        # print(me.last_name)
        # print(me.premium)
        # print(me.status)
        # print(me.usernames)

        target_dialog_title = config("telegram_group_name")
        messages_data = []
        telegram_channels = []
        pass_regex = r'```\s*([^\s`]+)\s*```'


        async for dialog in client.iter_dialogs():
            chanel_name = dialog.name
            chanel_id = dialog.id

            safe_name = sanitize_filename(chanel_name)

            download_folder = f"telegram_download/{safe_name}"


            os.makedirs(download_folder, exist_ok=True)

            # telegram_channels.append({
            #     'dialog_id': dialog.id,
            #     'dialog_title': dialog.title,
            #     # 'dialog_entity': dialog.entity,
            # })
            # pl.DataFrame(telegram_channels).write_csv('telegram_channels.csv')

            # print(dialog.title)
            # print(dialog.id)
            if dialog.title == target_dialog_title:
                # print(f' Messages from {dialog.title}:')

                async for message in client.iter_messages(dialog.entity, limit=10):

                    # pass_match = re.search(boxed_pw_pass_regex, message.text) if message.text else None
                    #
                    # messages_data.append({
                    #     'message_id': message.id,
                    #     'sender': message.sender.username if message.sender else None,
                    #     'sender_id': message.sender.id if message.sender else None,
                    #     'date': message.date.isoformat() if message.date else None,
                    #     'message': message.text,
                    #     'has_photo': bool(message.photo),
                    #     'has_video': bool(message.video),
                    #     'has_document': bool(message.document),
                    #     'has_sticker': bool(message.sticker),
                    #     'dice': bool(message.dice),
                    #     'media': bool(message.media),
                    #     'file': bool(message.file),
                    #     'file_size': message.file.size if message.file else None,
                    #     'file_title': message.file.title if message.file else None, # Useless
                    #     'file_duration': message.file.duration if message.file else None, # Useless
                    #     'web_preview': bool(message.web_preview),
                    #     'voice': bool(message.voice),
                    #     'raw_text': message.raw_text,
                    #     'file_name': message.file.name if message.file else None,
                    #     'pass_value': pass_match.group(1) if pass_match else None,
                    # })

                    # print(f'Message ID {message.id}:')
        #             print(f'Message textr {message.text}:')
        #             print(f'Date {message.date}:')
        #             print(f'Message text {message.text}:')
        #             print(f'Sender ID {message.sender.id}:')
        #             print(f'==============================')
        #
        #             if message.photo:
        #                 print(" [Has photo")
        #             if message.video:
        #                 print(" [Has video")
        #             if message.document:
        #                 print(" [Has document")
        #             if message.sticker:
        #                 print(" [Has sticker")

        #         print(pl.DataFrame(messages_data).write_csv('telegram_test_data.csv'))

                    if message.photo:
                        # path = await message.download_media(file="telegram_download")
                        path = await message.download_media(download_folder)

                        print('File saved to', path)
                    # if message.file:
                    #     path = await message.download_media(file="telegram_download")
                    #     print('File saved to', path)
        # # return me


if __name__ == "__main__":
    asyncio.run(main())

