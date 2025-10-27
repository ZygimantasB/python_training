from telethon import TelegramClient
from decouple import config

api_id = config("telegram_api_id")
api_hash = config("telegram_api_hash")

client = TelegramClient('session_name.session', api_id, api_hash)

async def main():
    # Getting information about yourself
    me = await client.get_me()

    # print(me.stringify())

    username = me.username
    print(username)
    print(me.phone)

    # async for dialog in client.iter_dialogs():
    #     print(dialog.name)

    await client.send_message('me', 'Hello World!')

    # message = await client.send_message(
    #     'me',
    #     'This message has **bold**, `code`, __italics__ and '
    #     'a [nice website](https://example.com)!',
    #     link_preview=False
    # )
    # print(message.raw_text)

    async for message in client.iter_messages('me'):
        print(message.id, message.text)

        if message.photo:
            path = await message.download_media()
            print('File saved to', path)

with client:
    client.loop.run_until_complete(main())
