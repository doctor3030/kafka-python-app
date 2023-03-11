import enum
import sys
import asyncio
import signal
import json
# import time
from typing import Optional, Any
import pydantic
import redis
import random
from dateutil import parser
from unittest import IsolatedAsyncioTestCase

sys.path.append('../')
from kafka_python_app.app import AppConfig, KafkaApp, MessagePipeline, MessageTransaction, \
    TransactionPipeResultOptions, EmitWithResponseOptions
from kafka_python_app.connector import KafkaConnector, ProducerRecord
from loguru_logger_lite import Logger, Sink, Sinks, BaseSinkOptions, LogLevels

KILL = False


class GracefulKiller:
    objs = []

    def exit_gracefully(self, *args):
        global KILL
        KILL = True
        if len(self.objs) > 1:
            for obj in self.objs:
                obj.close()


class LogMessage(pydantic.BaseModel):
    service_name: str
    service_id: str
    module: str
    component: str
    pid: int
    level: str
    timestamp: float
    text: str


class Events(enum.Enum):
    PROCESS_PERSON = 'process_person'
    PERSON_ADD_MIDDLE_NAME = 'person_add_middle_name'
    PERSON_MULTIPLY_AGE = 'person_multiply_age'
    PERSON_PROCESSED = 'person_processed'

    PROCESS_COMPANY = 'process_company'
    COMPANY_ADD_INC = 'company_add_inc'
    COMPANY_DOUBLE_STOCK_PRICE = 'company_double_stock_price'
    COMPANY_PROCESSED = 'company_processed'


class Topics(enum.Enum):
    APP_1 = 'test_topic_1'
    APP_2 = 'test_topic_2'
    APP_3 = 'test_topic_3'


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


async def person_add_middle_name(message, logger, **kwargs):
    if not message.get('error'):
        person = PersonPayload(**message['payload'])
        devider = kwargs.get('devider')
        middle_name = kwargs.get('middle_name')
        person.first_name = devider.join(person.first_name.split(devider) + [middle_name])
        message['payload'] = json.loads(person.json(exclude_unset=True))
    await asyncio.sleep(random.randint(1, 5) / 10)
    logger.info(f'Executing transaction: "person_add_middle_name" ==> result: {message}')
    return message


async def person_multiply_age(message, logger, **kwargs):
    if not message.get('error'):
        person = PersonPayload(**message['payload'])
        multiplier = kwargs.get('multiplier')
        person.age = person.age * multiplier
        message['payload'] = json.loads(person.json(exclude_unset=True))
    await asyncio.sleep(random.randint(1, 5) / 10)
    logger.info(f'Executing transaction: "person_multiply_age" ==> result: {message}')
    return message


async def company_add_inc(message, logger, **kwargs):
    if not message.get('error'):
        company = CompanyPayload(**message['payload'])
        company.name = company.name + ' INC.'
        message['payload'] = json.loads(company.json(exclude_unset=True))
    await asyncio.sleep(random.randint(1, 5) / 10)
    logger.info(f'Executing transaction: "company_add_inc" ==> result: {message}')
    return message


async def company_double_stock_price(message, logger: Optional[Any], **kwargs):
    if not message.get('error'):
        company = CompanyPayload(**message['payload'])
        company.stock_value = company.stock_value * 2
        message['payload'] = json.loads(company.json(exclude_unset=True))
    await asyncio.sleep(random.randint(1, 5) / 10)
    logger.info(f'Executing transaction: "company_double_stock_price" ==> result: {message}')
    return message


class TestKafkaApp(IsolatedAsyncioTestCase):
    LOGGER_STDOUT_FMT = "SERVICE NAME: <yellow>{extra[service_name]}</yellow> " \
                        "| SERVICE ID: <green>{extra[service_id]}</green> " \
                        "| MODULE: <green>{module}</green> | COMPONENT: <yellow>{name}</yellow> " \
                        "| PID: {process} | <level>{level}</level> | {time} | <level>{message}</level>"
    LOGGER_PLAIN_FMT = "SERVICE NAME: {extra[service_name]} | SERVICE ID: {extra[service_id]} " \
                       "| MODULE: {module} | COMPONENT: {name} | PID: {process} | {level} | {time} | {message}"

    @staticmethod
    def log_serializer(data):
        formatted = LogMessage(
            service_name=data.split('|')[0].split(':')[1].strip(),
            service_id=data.split('|')[1].split(':')[1].strip(),
            module=data.split('|')[2].split(':')[1].strip(),
            component=data.split('|')[3].split(':')[1].strip(),
            pid=data.split('|')[4].split(':')[1].strip(),
            level=data.split('|')[5].strip(),
            timestamp=parser.parse(data.split('|')[6].strip()).timestamp() * 1000,
            text=data.split('|')[7].strip()
        )
        return formatted.json(exclude_unset=True).encode('utf-8')

    LOGGER_0 = Logger.get_logger([
        Sink(
            name=Sinks.STDOUT,
            opts=BaseSinkOptions(
                level=LogLevels.DEBUG,
                format=LOGGER_STDOUT_FMT,
            )
        ),
    ])
    LOGGER_1 = Logger.get_logger([
        Sink(
            name=Sinks.STDOUT,
            opts=BaseSinkOptions(
                level=LogLevels.DEBUG,
                format=LOGGER_STDOUT_FMT,
            )
        ),
    ])
    LOGGER_2 = Logger.get_logger([
        Sink(
            name=Sinks.STDOUT,
            opts=BaseSinkOptions(
                level=LogLevels.DEBUG,
                format=LOGGER_STDOUT_FMT,
            )
        ),
    ])
    LOGGER_3 = Logger.get_logger([
        Sink(
            name=Sinks.STDOUT,
            opts=BaseSinkOptions(
                level=LogLevels.DEBUG,
                format=LOGGER_STDOUT_FMT,
            )
        ),
    ])

    LOGGER_0 = LOGGER_0.bind(service_name='Tester', service_id='tester')
    LOGGER_1 = LOGGER_1.bind(service_name='App 1', service_id='app_1')
    LOGGER_2 = LOGGER_2.bind(service_name='App 2', service_id='app_2')
    LOGGER_3 = LOGGER_3.bind(service_name='App 3', service_id='app_3')

    KAFKA_BOOTSTRAP_SERVERS = ['127.0.0.1:9092']
    CACHE_SERVER_ADDRESS = '127.0.0.1:6379'
    CACHE_SERVER_PW = 'pass'
    PIPED_EVENT_RETURN_TIMEOUT = 10

    cache_server_ip, cache_server_port = CACHE_SERVER_ADDRESS.split(':')
    event_cache_client = redis.Redis(
        host=cache_server_ip,
        port=int(cache_server_port),
        password=CACHE_SERVER_PW,
        db=0)

    # Scenario 1
    # App_1 receives event 'process_person' (test_topic_1)
    # App_1 pipes event 'person_add_middle_name' to App_2 (test_topic_2)
    # App_2 pipes event 'person_multiply_age' to App_3 (test_topic_3)
    # App_3 pipes event 'person_processed' back to App_1 (test_topic_1)
    # App_1 receives event (standalone event handler) 'person_processed' and prints result

    # Scenario 2
    # App_1 receives event 'process_company'
    # App_1 pipes event with return 'company_add_inc' to App_2
    # App_2 pipes event 'company_add_inc' back to App_1
    # App_1 pipes event with return 'company_double_stock_price' to App_3
    # App_3 pipes event 'company_processed' back to App_1
    # App_1 prints result

    app2_person_mid_name_pipeline = MessagePipeline(
        transactions=[
            MessageTransaction(
                fnc=person_add_middle_name,
                args={
                    'devider': '-',
                    'middle_name': 'Joe'
                },
                pipe_result_options=TransactionPipeResultOptions(
                    pipe_event_name=Events.PERSON_ADD_MIDDLE_NAME.value,
                    pipe_to_topic=Topics.APP_1.value
                )
            )
        ],
        logger=LOGGER_2
    )
    app2_person_age_pipeline = MessagePipeline(
        transactions=[
            MessageTransaction(
                fnc=person_multiply_age,
                args={
                    'multiplier': 2
                },
                pipe_result_options=TransactionPipeResultOptions(
                    pipe_event_name=Events.PERSON_MULTIPLY_AGE.value,
                    pipe_to_topic=Topics.APP_1.value
                )
            )
        ],
        logger=LOGGER_2
    )

    app3_company_add_inc_pipeline = MessagePipeline(
        transactions=[
            MessageTransaction(
                fnc=company_add_inc,
                pipe_result_options=TransactionPipeResultOptions(
                    pipe_event_name=Events.COMPANY_ADD_INC.value,
                    pipe_to_topic=Topics.APP_1.value
                )
            ),
        ],
        logger=LOGGER_2
    )
    app3_company_stock_price_pipeline = MessagePipeline(
        transactions=[
            MessageTransaction(
                fnc=company_double_stock_price,
                pipe_result_options=TransactionPipeResultOptions(
                    pipe_event_name=Events.COMPANY_DOUBLE_STOCK_PRICE.value,
                    pipe_to_topic=Topics.APP_1.value
                )
            ),
        ],
        logger=LOGGER_3
    )

    app2_pipelines_map = {
        Events.PERSON_ADD_MIDDLE_NAME.value: app2_person_mid_name_pipeline,
        Events.PERSON_MULTIPLY_AGE.value: app2_person_age_pipeline
    }
    app3_pipelines_map = {
        Events.COMPANY_ADD_INC.value: app3_company_add_inc_pipeline,
        Events.COMPANY_DOUBLE_STOCK_PRICE.value: app3_company_stock_price_pipeline
    }

    app1_config = AppConfig(
        app_name='App 1',
        app_id='app_1',
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        consumer_config={
            'group_id': 'test_app1_group'
        },
        listen_topics=[Topics.APP_1.value],
        emit_with_response_options=EmitWithResponseOptions(
            topic_event_list=[
                (Topics.APP_1.value, Events.PERSON_ADD_MIDDLE_NAME.value),
                (Topics.APP_1.value, Events.PERSON_MULTIPLY_AGE.value),
                (Topics.APP_1.value, Events.COMPANY_ADD_INC.value),
                (Topics.APP_1.value, Events.COMPANY_DOUBLE_STOCK_PRICE.value),
            ],
            cache_client=event_cache_client,
            return_event_timeout=30
        ),
        logger=LOGGER_1
    )
    app2_config = AppConfig(
        app_name='App 2',
        app_id='app_2',
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        consumer_config={
            'group_id': 'test_app2_group'
        },
        listen_topics=[Topics.APP_2.value],
        pipelines_map=app2_pipelines_map,
        logger=LOGGER_2
    )
    app3_config = AppConfig(
        app_name='App 3',
        app_id='app_3',
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        consumer_config={
            'group_id': 'test_app3_group'
        },
        listen_topics=[Topics.APP_3.value],
        pipelines_map=app3_pipelines_map,
        logger=LOGGER_3
    )

    producer = KafkaConnector.get_producer(KAFKA_BOOTSTRAP_SERVERS)
    app_1 = KafkaApp(app1_config)
    app_2 = KafkaApp(app2_config)
    app_3 = KafkaApp(app3_config)
    killer.objs = [app_1, app_2, app_3]

    messages = [
        {
            'event': Events.PERSON_ADD_MIDDLE_NAME.value,
            'payload': {
                'first_name': 'John',
                'last_name': 'Doe',
                'age': 35
            }
        },
        {
            'event': Events.PERSON_MULTIPLY_AGE.value,
            'payload': {
                'first_name': 'John',
                'last_name': 'Doe',
                'age': 35
            }
        },
        {
            'event': Events.COMPANY_ADD_INC.value,
            'payload': {
                'name': 'SomeCompany',
                'stock_value': 1224.55
            }
        },
        {
            'event': Events.COMPANY_DOUBLE_STOCK_PRICE.value,
            'payload': {
                'name': 'SomeCompany',
                'stock_value': 1224.55
            }
        },
    ]

    async def calls(self):
        await asyncio.sleep(1)
        res = await self.app_1.emit_with_response(
            Topics.APP_2.value,
            ProducerRecord(value=self.messages[0])
        )
        res = PersonPayload(**res['payload'])

        await asyncio.sleep(1)
        print(await self.app_1.emit_with_response(
            Topics.APP_2.value,
            ProducerRecord(value=self.messages[1])
        ))
        await asyncio.sleep(1)
        print(await self.app_1.emit_with_response(
            Topics.APP_3.value,
            ProducerRecord(value=self.messages[2])
        ))
        await asyncio.sleep(1)
        print(await self.app_1.emit_with_response(
            Topics.APP_3.value,
            ProducerRecord(value=self.messages[3])
        ))
        self.LOGGER_0.info('Processing completed. Closing...')
        # await asyncio.sleep(10)
        self.app_1.close()
        self.app_2.close()
        self.app_3.close()

    async def test(self):
        await asyncio.gather(self.app_1.run(), self.app_2.run(), self.app_3.run(), self.calls())
