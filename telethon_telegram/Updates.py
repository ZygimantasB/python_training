import logging
from decouple import config

logging.basicConfig(format='[%(levelname) %(asctime)s] %(name)s: %(message)s',
                    level=logging.WARNING)

api_id = config("telegram_api_id")
api_hash = config("telegram_api_hash")

from telethon import TelegramClient, events

# client = TelegramClient('session_name.session', api_id=api_id, api_hash=api_hash)
#
# @client.on(events.NewMessage)
# async def my_event_handler(event):
#     if 'hello' in event.raw_text:
#         await event.reply("Hello World!")
#
# client.start()
# client.run_until_disconnected()

# ----------------------------


