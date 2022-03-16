import enum
import sys
import asyncio
import signal
import json
from typing import Dict, Union, List, Optional, Any
import pydantic
from unittest import IsolatedAsyncioTestCase

sys.path.append('/home/doc/Documents/Projects/kafka-app')
from kafka_app.app import KafkaConfig, AppConfig, KafkaApp
from kafka_app.kafka_connector import KafkaConnector
from loguru_logger import Logger


KILL = False


class GracefulKiller:
    obj = None

    def exit_gracefully(self, *args):
        global KILL
        KILL = True
        if self.obj:
            self.obj.close()


class Events(enum.Enum):
    PROCESS_PERSON = 'process_person'
    PROCESS_COMPANY = 'process_company'
    PROCESS_PERSON_ASYNC = 'process_person_async'
    PROCESS_COMPANY_ASYNC = 'process_company_async'


class PersonPayload(pydantic.BaseModel):
    firs_name: str
    last_name: str
    age: int


class CompanyPayload(pydantic.BaseModel):
    name: str
    stock_value: float


class Message(pydantic.BaseModel):
    event: str
    payload: Any


killer = GracefulKiller()
signal.signal(signal.SIGINT, killer.exit_gracefully)
signal.signal(signal.SIGTERM, killer.exit_gracefully)

msg_counter = 0


class TestKafkaApp(IsolatedAsyncioTestCase):
    _cls_logger = Logger()
    LOGGER = _cls_logger.default_logger
    # KAFKA_BOOTSTRAP_SERVERS = ['10.0.0.74:9092']
    KAFKA_BOOTSTRAP_SERVERS = ['192.168.2.190:9092']
    TEST_TOPIC = 'test_topic'

    kafka_config = KafkaConfig(**{
        'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
        'consumer_config': {
            'group_id': 'test_app_group'
        },
        'listen_topics': [TEST_TOPIC],
        'message_cls': {TEST_TOPIC: Message}
    })
    config = AppConfig(**{
        'app_name': 'Test application',
        'kafka_config': kafka_config,
        'logger': LOGGER
    })

    producer = KafkaConnector.get_producer(KAFKA_BOOTSTRAP_SERVERS)
    app = KafkaApp(config)
    killer.obj = app

    async def watch_counter(self):
        while True:
            if msg_counter > 0:
                self.LOGGER.info('Received {} message and now closing...'.format(msg_counter))
                self.app.close()
                break
            if KILL:
                break
            await asyncio.sleep(0.1)
        self.producer.close()
        # self.LOGGER.close()

    async def test_handle(self):
        @self.app.on(Events.PROCESS_PERSON.value)
        def handle_person(message, **kwargs):
            global msg_counter
            print('Handling "process_person" event..')
            print('Received: {}\n'.format(message))
            msg_counter += 1

        @self.app.on(Events.PROCESS_COMPANY.value)
        def handle_company(message, **kwargs):
            global msg_counter
            print('Handling "process_company" event..')
            print('Received: {}\n'.format(message))
            msg_counter += 1

        @self.app.on(Events.PROCESS_PERSON_ASYNC.value)
        async def handle_person(message, **kwargs):
            global msg_counter
            print('Handling "process_person_async" event..')
            print('Received: {}\n'.format(message))
            msg_counter += 1

        @self.app.on(Events.PROCESS_COMPANY_ASYNC.value)
        async def handle_company(message, **kwargs):
            global msg_counter
            print('Handling "process_company_async" event..')
            print('Received: {}\n'.format(message))
            msg_counter += 1

        messages = [
            {
                'event': Events.PROCESS_PERSON.value,
                'payload': {
                    'first_name': 'John',
                    'last_name': 'Doe',
                    'age': 35
                }
            },
            {
                'event': Events.PROCESS_COMPANY.value,
                'payload': {
                    'name': 'SomeCompany',
                    'stock_value': 1224.55
                }
            },
            {
                'event': Events.PROCESS_PERSON_ASYNC.value,
                'payload': {
                    'first_name': 'John Async',
                    'last_name': 'Doe',
                    'age': 15
                }
            },
            {
                'event': Events.PROCESS_COMPANY_ASYNC.value,
                'payload': {
                    'name': 'SomeCompany Async',
                    'stock_value': 12424.55
                }
            }
        ]

        for msg_obj in messages:
            msg = Message(**msg_obj)
            await asyncio.sleep(0.5)
            self.producer.send(self.TEST_TOPIC, json.loads(msg.json(exclude_unset=True)))

        # msg = Message(**msg_person)
        # self.producer.send(self.TEST_TOPIC, json.loads(msg.json(exclude_unset=True)))
        #
        # msg = Message(**msg_company)
        # self.producer.send(self.TEST_TOPIC, json.loads(msg.json(exclude_unset=True)))

        await asyncio.sleep(0.1)
        await asyncio.gather(self.app.run(), self.watch_counter())

