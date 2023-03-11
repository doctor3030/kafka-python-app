import enum
import sys
import asyncio
import signal
import json
from typing import Optional, Any
import pydantic
from unittest import IsolatedAsyncioTestCase

sys.path.append('../')
from kafka_python_app.app import AppConfig, KafkaApp, MessagePipeline, MessageTransaction
from kafka_python_app.connector import KafkaConnector
from loguru_logger_lite import Logger


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
    first_name: str
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


def person_add_middle_name(message, logger, **kwargs):
    person = PersonPayload(**message['payload'])
    devider = kwargs.get('devider')
    middle_name = kwargs.get('middle_name')
    person.first_name = devider.join(person.first_name.split(devider) + [middle_name])
    logger.info(f'Executing pipeline: "person_add_middle_name" ==> result: {person}')
    print(kwargs.get('headers'))
    return message


def person_multiply_age(message, logger, **kwargs):
    person = PersonPayload(**message['payload'])
    multiplier = kwargs.get('multiplier')
    person.age = person.age * multiplier
    logger.info(f'Executing pipeline: "person_multiply_age" ==> result: {person}')
    print(kwargs.get('headers'))
    return message


def company_add_inc(message, logger, **kwargs):
    company = CompanyPayload(**message['payload'])
    company.name = company.name + ' INC.'
    logger.info(f'Executing pipeline: "company_add_inc" ==> result: {company}')
    print(kwargs.get('headers'))
    return message


def company_double_stock_price(message, logger: Optional[Any], **kwargs):
    company = CompanyPayload(**message['payload'])
    company.stock_value = company.stock_value * 2
    logger.info(f'Executing pipeline: "company_double_stock_price" ==> result: {company}')
    print(kwargs.get('headers'))
    return message


class TestKafkaApp(IsolatedAsyncioTestCase):
    LOGGER = Logger.get_default_logger()
    KAFKA_BOOTSTRAP_SERVERS = ['127.0.0.1:9092']
    TEST_TOPIC = 'test_topic'

    person_pipeline = MessagePipeline(
        transactions=[
            MessageTransaction(
                fnc=person_add_middle_name,
                args={
                    'devider': '-',
                    'middle_name': 'Joe'
                }
            ),
            MessageTransaction(
                fnc=person_multiply_age,
                args={
                    'multiplier': 2
                }
            )
        ],
        logger=LOGGER
    )
    company_pipeline = MessagePipeline(
        transactions=[
            MessageTransaction(fnc=company_add_inc),
            MessageTransaction(fnc=company_double_stock_price)
        ],
        logger=LOGGER
    )
    pipelines_map = {
        Events.PROCESS_PERSON.value: person_pipeline,
        Events.PROCESS_COMPANY.value: company_pipeline
    }

    config = AppConfig(
        app_name='Test application',
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        consumer_config={
            'group_id': 'test_app_group'
        },
        listen_topics=[TEST_TOPIC],
        message_value_cls={TEST_TOPIC: Message},
        pipelines_map=pipelines_map,
        logger=LOGGER
    )

    producer = KafkaConnector.get_producer(KAFKA_BOOTSTRAP_SERVERS)
    app = KafkaApp(config)
    killer.obj = app

    async def watch_counter(self):
        while True:
            if msg_counter >= 4:
                self.LOGGER.info('Received {} message and now closing...'.format(msg_counter))
                self.app.close()
                break
            if KILL:
                break
            await asyncio.sleep(0.001)

    async def test_handle(self):
        @self.app.on(Events.PROCESS_PERSON.value)
        def handle_person(message, **kwargs):
            global msg_counter
            self.LOGGER.info('Handling "process_person" event..')
            self.LOGGER.info('Received: {}\n'.format(message))
            msg_counter += 1

        @self.app.on(Events.PROCESS_COMPANY.value)
        def handle_company(message, **kwargs):
            global msg_counter
            self.LOGGER.info('Handling "process_company" event..')
            self.LOGGER.info('Received: {}\n'.format(message))
            msg_counter += 1

        @self.app.on(Events.PROCESS_PERSON_ASYNC.value)
        async def handle_person(message, **kwargs):
            global msg_counter
            self.LOGGER.info('Handling "process_person_async" event..')
            self.LOGGER.info('Received: {}\n'.format(message))
            msg_counter += 1

        @self.app.on(Events.PROCESS_COMPANY_ASYNC.value)
        async def handle_company(message, **kwargs):
            global msg_counter
            self.LOGGER.info('Handling "process_company_async" event..')
            self.LOGGER.info('Received: {}\n'.format(message))
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
            await asyncio.sleep(0.001)
            self.producer.send(self.TEST_TOPIC, json.loads(msg.json(exclude_unset=True)), headers=[('event_id', '1111'.encode('utf-8'))])

        await asyncio.sleep(0.01)
        self.producer.close()
        await asyncio.gather(self.app.run(), self.watch_counter())

