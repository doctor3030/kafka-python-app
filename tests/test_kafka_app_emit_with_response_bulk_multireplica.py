import enum
import sys
import asyncio
import signal
import json
from typing import Optional, Any, List, Dict
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

# KILL = False
#
#
# class GracefulKiller:
#     objs = []
#
#     def exit_gracefully(self, *args):
#         global KILL
#         KILL = True
#         if len(self.objs) > 1:
#             for obj in self.objs:
#                 obj.close()


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
    PERSON_ADD_MIDDLE_NAME_INIT = 'person_add_middle_name_init'
    PERSON_MULTIPLY_AGE_INIT = 'person_multiply_age_init'

    COMPANY_ADD_INC_INIT = 'company_add_inc_init'
    COMPANY_DOUBLE_STOCK_PRICE_INIT = 'company_double_stock_price_init'


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


# killer = GracefulKiller()
# signal.signal(signal.SIGINT, killer.exit_gracefully)
# signal.signal(signal.SIGTERM, killer.exit_gracefully)


class TestKafkaApp(IsolatedAsyncioTestCase):
    # LOGGER_STDOUT_FMT = "SERVICE NAME: <yellow>{extra[service_name]}</yellow> " \
    #                     "| SERVICE ID: <green>{extra[service_id]}</green> " \
    #                     "| MODULE: <green>{module}</green> | COMPONENT: <yellow>{name}</yellow> " \
    #                     "| PID: {process} | <level>{level}</level> | {time} | <level>{message}</level>"
    # LOGGER_PLAIN_FMT = "SERVICE NAME: {extra[service_name]} | SERVICE ID: {extra[service_id]} " \
    #                    "| MODULE: {module} | COMPONENT: {name} | PID: {process} | {level} | {time} | {message}"

    # @staticmethod
    # def log_serializer(data):
    #     formatted = LogMessage(
    #         service_name=data.split('|')[0].split(':')[1].strip(),
    #         service_id=data.split('|')[1].split(':')[1].strip(),
    #         module=data.split('|')[2].split(':')[1].strip(),
    #         component=data.split('|')[3].split(':')[1].strip(),
    #         pid=data.split('|')[4].split(':')[1].strip(),
    #         level=data.split('|')[5].strip(),
    #         timestamp=parser.parse(data.split('|')[6].strip()).timestamp() * 1000,
    #         text=data.split('|')[7].strip()
    #     )
    #     return formatted.model_dump_json(exclude_unset=True).encode('utf-8')
    #
    # LOGGER_0 = Logger.get_logger([
    #     Sink(
    #         name=Sinks.STDOUT,
    #         opts=BaseSinkOptions(
    #             level=LogLevels.DEBUG,
    #             format=LOGGER_STDOUT_FMT,
    #         )
    #     ),
    # ])
    # LOGGER_0 = LOGGER_0.bind(service_name='Tester', service_id='tester')

    KAFKA_BOOTSTRAP_SERVERS = ['127.0.0.1:9092']
    # CACHE_SERVER_ADDRESS = '127.0.0.1:6379'
    # CACHE_SERVER_PW = 'pass'
    # PIPED_EVENT_RETURN_TIMEOUT = 10
    #
    # app_config = AppConfig(
    #     app_name='Tester',
    #     app_id='tester',
    #     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    #     producer_config={
    #         'compression_type': 'gzip',
    #         'value_serializer': lambda x: x.encode('utf-8'),
    #         'max_request_size': 1048576000,
    #         'batch_size': 0
    #     },
    #     consumer_config={
    #         'group_id': 'test_app1_group',
    #         'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
    #     },
    #     listen_topics=[Topics.APP_1.value],
    #     logger=LOGGER_0
    # )

    producer = KafkaConnector.get_producer(KAFKA_BOOTSTRAP_SERVERS)

    messages: List[Dict] = [
        {
            'event': Events.PERSON_ADD_MIDDLE_NAME_INIT.value,
            'payload': {
                'first_name': 'John',
                'last_name': 'Doe',
                'age': 35
            }
        },
        {
            'event': Events.PERSON_MULTIPLY_AGE_INIT.value,
            'payload': {
                'first_name': 'John',
                'last_name': 'Doe',
                'age': 35
            }
        },
        {
            'event': Events.COMPANY_ADD_INC_INIT.value,
            'payload': {
                'name': 'SomeCompany',
                'stock_value': 1224.55
            }
        },
        {
            'event': Events.COMPANY_DOUBLE_STOCK_PRICE_INIT.value,
            'payload': {
                'name': 'SomeCompany',
                'stock_value': 1224.55
            }
        },
    ]

    def test(self):
        for _ in range(1000):
            for msg in self.messages:
                self.producer.send(Topics.APP_1.value, msg)

