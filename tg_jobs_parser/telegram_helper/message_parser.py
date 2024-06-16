import asyncio
from datetime import datetime
from tg_jobs_parser.configs import TelegramConfig
from tg_jobs_parser.telegram_helper.telegram_client import TelegramClient
from tg_jobs_parser.configs import vars

def each_slice(arr, n):
    """Генерирует слайсы по n элементов."""
    for i in range(0, len(arr), n):
        yield arr[i:i + n]
def process_msg(message):
    message_text = message.text if message.text else ""
    message_caption = message.caption if message.caption else ""
    return f"{message_text} {message_caption}".strip()

def generate_ids(from_msg_id, limit, flow):
    if flow == 'right':
        return [from_msg_id + i for i in range(limit)]
    elif flow == 'left':
        return [from_msg_id - i for i in range(limit)]
    else:
        raise ValueError("flow must be 'right' or 'left'")

# Примеры использования:


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
        from_msg_id = channel['right_saved_id'] + 1
        limit = int(channel['last_posted_message_id']) - int(from_msg_id)
    elif channel['left_saved_id'] > channel['target_id']:
        flow = 'left'
        from_msg_id = channel['left_saved_id'] - 1
        limit = int(from_msg_id) - int(channel['target_id'])
    else:
        print(f"UNCOVERED CASE => nothing to do at {channel['id']}")
        raise
    limit = max(limit, 1)
    return from_msg_id, limit, flow


class MessageParser:
    def __init__(self):
        self.left = None
        self.right = None
        self.msgs = None
        self.message_data = None
        self.config = TelegramConfig()
        self.client = TelegramClient(session_string=self.config.get_session())
        # self.client.test_client()
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
        limit = min(limit, vars.MAX_LIMIT)
        try:
            msgs_ids = generate_ids(from_msg_id, limit, flow)
            print(
                f"Reading messages from chat {channel_metadata['id']} from: {from_msg_id} limit: {limit}, flow: {flow}")
            messages = {}
            async with self.app:
                for slice_ids in each_slice(msgs_ids, 10):
                    msgs = await self.app.get_messages(int(channel_metadata['id']), slice_ids)
                    for message in msgs:
                        if message.empty:
                            msg_data = {
                                'id': message.id,
                                'empty': True,
                                'text': None,
                                'datetime': None,
                                'date': None,
                                'link': None,
                                'sender_chat_id': None,
                                'sender_chat_user_name': None,
                                'user': None,
                                'user_name': None,
                                'chat_id': channel_metadata['id'],
                            }
                        else:
                            msg_data = {
                                'id': message.id,
                                'empty': False,
                                'text': process_msg(message),
                                'datetime': str(message.date),
                                'date': str(message.date.date()),
                                'link': message.link,
                                'sender_chat_id': message.sender_chat.id if message.sender_chat else None,
                                'sender_chat_user_name': message.sender_chat.username if message.sender_chat else None,
                                'user': message.from_user.id if message.from_user else None,
                                'user_name': f'@{message.from_user.username}' if message.from_user else None,
                                'chat_id': channel_metadata['id'],
                            }
                            if msg_data['text'] == '':
                                msg_data['empty'] = True
                        print(msg_data)
                        messages[msg_data['id']] = msg_data
                    await asyncio.sleep(1)
            self.msgs = messages.values()
            self.left = 0 if len(msgs_ids) == 0 else msgs_ids[0]
            self.right = 0 if len(msgs_ids) == 0 else msgs_ids[-1]
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
