import asyncio
from datetime import datetime
from tg_jobs_parser.configs import TelegramConfig
from tg_jobs_parser.telegram_helper.telegram_client import TelegramClient
from tg_jobs_parser.configs import vars


def process_msg(message):
    message_text = message.text if message.text else ""
    message_caption = message.caption if message.caption else ""
    return f"{message_text} {message_caption}".strip()


def chat_parser_args(channel):
    if channel['right_saved_id'] is None and channel['left_saved_id'] is None:
        flow = 'right'
        from_msg_id = channel['target_id']
        limit = int(channel['last_posted_message_id']) - int(from_msg_id)
    elif channel['right_saved_id'] is None or channel['left_saved_id'] is None:
        print(f"INCORRECT CASE => nothing to do at {channel['id']} CHECK THE DATA")
        raise
    elif channel['right_saved_id'] >= channel['last_posted_message_id'] \
            and channel['left_saved_id'] <= channel['target_id']:
        print(f"nothing to do at {channel['id']}")
        return
    elif channel['right_saved_id'] < channel['last_posted_message_id']:
        flow = 'right'
        from_msg_id = channel['right_saved_id']
        limit = int(channel['last_posted_message_id']) - int(from_msg_id)
    elif channel['left_saved_id'] > channel['target_id']:
        flow = 'left'
        from_msg_id = channel['left_saved_id']
        limit = int(from_msg_id) - int(channel['target_id'])
    else:
        print(f"UNCOVERED CASE => nothing to do at {channel['id']}")
        raise
    return from_msg_id, limit, flow


class MessageParser:
    def __init__(self):
        self.left = None
        self.right = None
        self.msgs = None
        self.message_data = None
        self.config = TelegramConfig()
        self.client = TelegramClient(session_string=self.config.get_session())
        self.client.test_client()
        self.app = self.client.get_client()
        self.channels = {}

    async def get_message_id(self, chat_id, target_date):
        self.message_data = []
        print(f'finding id of msg for {target_date} in chat {chat_id}')
        try:
            date = datetime.strptime(target_date, "%Y-%m-%d")
            async with self.app as app:
                async for message in self.app.get_chat_history(int(chat_id), limit=1, offset_date=date):
                    await asyncio.sleep(1)
                    print(message.id, str(message.date))
                    self.message_data = [message.id or None, str(message.date) or None]
        finally:
            print(f'{self} done')

    async def parse_channel_messages(self, channel_metadata):
        self.msgs = None
        self.left = None
        self.right = None
        from_msg_id, limit, flow = chat_parser_args(channel_metadata)
        print(channel_metadata['id'], from_msg_id, limit, flow)
        # flow new = from target_id or right border  to last_posted_message_id. from  < to
        # flow old = from left border or last to target. from > to
        try:
            kwargs = {'limit': min(limit, vars.MAX_LIMIT), 'offset_id': int(from_msg_id)}
            if flow == 'right':
                kwargs['offset_id'] += 1
                kwargs['offset'] = -kwargs['limit']

            kwargs['limit'] = max(kwargs['limit'], 1)
            print(f"Reading messages  from chat {channel_metadata['id']} with {kwargs}")
            messages = []
            async with self.app:
                async for message in self.app.get_chat_history(int(channel_metadata['id']), **kwargs):
                    msg_data = {
                        'id': message.id,
                        'text': process_msg(message),
                        'datetime': str(message.date),
                        'date': str(message.date.date()),
                        'link': message.link,
                        'user': message.from_user.id if message.from_user else None,
                        'user_name': f'@{message.from_user.username}' if message.from_user else None,
                        'chat_id': channel_metadata['id'],
                    }
                    print(msg_data)
                    messages.append(msg_data)
                    await asyncio.sleep(0.2)  # Пауза между запросами, чтобы не нагружать сервер
            unique_messages = {item['id']: item for item in messages}.values()
            await asyncio.sleep(1)
            self.msgs = sorted(unique_messages, key=lambda x: x['id'])
            if flow == 'right':
                self.left = from_msg_id + 1
                self.right = self.msgs[-1]['id']
            else:
                self.right = from_msg_id - 1
                self.left = self.msgs[0]['id']
        finally:
            print(f'{self} done')

    def run_msg_parser(self, chat_id, date):
        self.app.run(self.get_message_id(chat_id, date))
        return self.message_data

    def run_chat_parser(self, channel_metadata):
        self.app.run(self.parse_channel_messages(channel_metadata))
        return self.msgs, self.left, self.right

# parser = MessageParser()
# result1 = parser.run_msg_parser('-1001102660677', '2024-02-02') # ok
# result2 = parser.run_msg_parser('93372553', '2024-02-10') # error
