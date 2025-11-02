import asyncio
import polars as pl

from pprint import pprint
from telethon import TelegramClient, events
from decouple import config

api_id = config("telegram_api_id")
api_hash = config("telegram_api_hash")

client = TelegramClient('session_name', api_id=api_id, api_hash=api_hash)

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

        target_dialog_title = "SKELBIMAI"
        messages_data = []

        async for dialog in client.iter_dialogs():
            # print(dialog.title)
            # print(dialog.id)
            if dialog.title == target_dialog_title:
                # print(f' Messages from {dialog.title}:')

                async for message in client.iter_messages(dialog.entity, limit=10):
                    messages_data.append({
                        'message_id': message.id,
                        'sender': message.sender.username,
                        'sender_id': message.sender.id,
                        'date': message.date,
                        'message': message.text,
                        'has_photo': message.photo,
                        'has_video': message.video,
                        'has_document': message.document,
                        'has_sticker': message.sticker,
                        'dice': message.dice,
                    })
                print(f'Message ID {message.id}:')
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
                print(pl.DataFrame(messages_data))
        # # return me


if __name__ == "__main__":
    asyncio.run(main())
