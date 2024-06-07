import asyncio
from datetime import datetime, timezone
from configs.telegram_config import TelegramConfig
from pyrogram import Client

MAX_LIMIT = 100


def process_msg(message):
    message_text = message.text if message.text else ""
    message_caption = message.caption if message.caption else ""
    return f"{message_text} {message_caption}".strip()

class MessageParser:
    def __init__(self):
        self.config = TelegramConfig()
        self.client = Client("my_account", api_id=self.config.get_api_id(), api_hash=self.config.get_api_hash())
        self.client.start()

    async def get_message_id(self, chat_id, target_date):
        print(f'finding id of msg for {target_date} in chat {chat_id}')
        try:
            date = datetime.strptime(target_date, "%Y-%m-%d")
            async for message in self.client.get_chat_history(chat_id, limit=1, offset_date=date):
                await asyncio.sleep(1)
                print(message.id, str(message.date))
                return message.id or None, str(message.date) or None
        finally:
            print(f'{self} done')

    async def parse_channel_messages(self, chat_id, from_msg, limit, flow='right'):
        print(chat_id, from_msg, limit, flow)
        # flow new = from target_id or right border  to last_posted_message_id. from  < to
        # flow old = from left border or last to target. from > to
        try:
            kwargs = {'limit': min(limit, MAX_LIMIT), 'offset_id': int(from_msg)}
            if flow == 'right':
                kwargs['offset_id'] += 1
                kwargs['offset'] = -kwargs['limit']

            kwargs['limit'] = max(kwargs['limit'], 1)
            print(f'Reading messages  from chat {chat_id} with {kwargs}')
            messages = []

            async for message in self.client.get_chat_history(chat_id, **kwargs):
                msg_data = {
                    'id': message.id,
                    'text': process_msg(message),
                    'datetime': str(message.date),
                    'date': str(message.date.date()),
                    'link': message.link,
                    'user': message.from_user.id if message.from_user else None,
                    'user_name': f'@{message.from_user.username}' if message.from_user else None,
                    'chat_id': chat_id,
                }
                print(msg_data)
                messages.append(msg_data)
                await asyncio.sleep(0.2)  # Пауза между запросами, чтобы не нагружать сервер
            unique_messages = {item['id']: item for item in messages}.values()
            await asyncio.sleep(1)
            return sorted(unique_messages, key=lambda x: x['id'])
        finally:
            # await self.client.stop()
            print(f'{self} done')

    def run_chat_parser(self, chat_id, from_msg_id, limit, flow):
        loop = asyncio.get_event_loop()
        task = loop.create_task(self.parse_channel_messages(chat_id, from_msg_id, limit, flow))
        return loop.run_until_complete(task)

    def run_msg_parser(self, chat_id, date):
        loop = asyncio.get_event_loop()
        task = loop.create_task(self.get_message_id(chat_id, date))
        return loop.run_until_complete(task)