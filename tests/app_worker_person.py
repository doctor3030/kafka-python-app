import enum
import sys
import asyncio
import time
import json
from typing import Any
import pydantic
import redis
import random
from dateutil import parser
import uuid

sys.path.append('../')
from kafka_python_app.app import AppConfig, KafkaApp, MessagePipeline, MessageTransaction, TransactionPipeResultOptions
from loguru_logger_lite import Logger, Sink, Sinks, BaseSinkOptions, LogLevels


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


class Topics(enum.Enum):
    APP_1 = 'test_topic_1'
    APP_2 = 'test_topic_2'
    APP_3 = 'test_topic_3'


class PersonPayload(pydantic.BaseModel):
    first_name: str
    last_name: str
    age: int


class Message(pydantic.BaseModel):
    event: str
    payload: Any


def log_serializer(data):
    formatted = LogMessage(
        service_name=data.split('|')[0].split(':')[1].strip(),
        service_id=data.split('|')[1].split(':')[1].strip(),
        module=data.split('|')[2].split(':')[1].strip(),
        component=data.split('|')[3].split(':')[1].strip(),
        pid=data.split('|')[4].split(':')[1].strip(),
        level=data.split('|')[5].strip(),
        timestamp=parser.parse(data.split('|')[6].strip()).timestamp() * 1000,
        text='|'.join(data.split('|')[7:]).strip()
    )
    return formatted.model_dump_json(exclude_unset=True).encode('utf-8')


async def person_add_middle_name(message, logger, **kwargs):
    if not message.get('error'):
        person = PersonPayload(**message['payload'])
        devider = kwargs.get('devider')
        middle_name = kwargs.get('middle_name')
        person.first_name = devider.join(person.first_name.split(devider) + [middle_name])
        message['payload'] = person.model_dump(exclude_unset=True)
    await asyncio.sleep(random.randint(1, 5) / 10)
    logger.info(f'Executing transaction: "person_add_middle_name" ==> result: {message}')
    return message


async def person_multiply_age(message, logger, **kwargs):
    if not message.get('error'):
        person = PersonPayload(**message['payload'])
        multiplier = kwargs.get('multiplier')
        person.age = person.age * multiplier
        message['payload'] = person.model_dump(exclude_unset=True)
    await asyncio.sleep(random.randint(1, 5) / 10)
    logger.info(f'Executing transaction: "person_multiply_age" ==> result: {message}')
    return message


SERVICE_ID = str(uuid.uuid4())
SERVICE_NAME = 'APP WORKER PERSON'

KAFKA_BOOTSTRAP_SERVERS = ['127.0.0.1:9092']
CACHE_SERVER_ADDRESS = '127.0.0.1:6379'
CACHE_SERVER_PW = 'pass'
PIPED_EVENT_RETURN_TIMEOUT = 10

LOGGER_STDOUT_FMT = "SERVICE NAME: <yellow>{extra[service_name]}</yellow> " \
                    "| SERVICE ID: <green>{extra[service_id]}</green> " \
                    "| MODULE: <green>{module}</green> | COMPONENT: <yellow>{name}</yellow> " \
                    "| PID: {process} | <level>{level}</level> | {time} | <level>{message}</level>"
LOGGER_PLAIN_FMT = "SERVICE NAME: {extra[service_name]} | SERVICE ID: {extra[service_id]} " \
                   "| MODULE: {module} | COMPONENT: {name} | PID: {process} | {level} | {time} | {message}"
LOGGER = Logger.get_logger([
    Sink(
        name=Sinks.STDOUT,
        opts=BaseSinkOptions(
            level=LogLevels.DEBUG,
            format=LOGGER_STDOUT_FMT,
        )
    ),
])
LOGGER = LOGGER.bind(service_name=SERVICE_NAME, service_id=SERVICE_ID)

cache_server_ip, cache_server_port = CACHE_SERVER_ADDRESS.split(':')
event_cache_client = redis.Redis(
    host=cache_server_ip,
    port=int(cache_server_port),
    password=CACHE_SERVER_PW,
    db=0)

person_mid_name_pipeline = MessagePipeline(
    name='person_mid_name_pipeline',
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
    logger=LOGGER
)
person_age_pipeline = MessagePipeline(
        name='person_age_pipeline',
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
        logger=LOGGER
    )

config = AppConfig(
    app_name=SERVICE_NAME,
    app_id=SERVICE_ID,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    producer_config={
        'compression_type': 'gzip',
        'value_serializer': lambda x: x.encode('utf-8'),
        'max_request_size': 1048576000,
        'batch_size': 0
    },
    consumer_config={
        'group_id': 'test_app2_group',
        'auto_offset_reset': 'latest',
        'enable_auto_commit': False,
        'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
        'max_poll_records': 10,
        'session_timeout_ms': 25000
    },
    listen_topics=[Topics.APP_2.value],
    pipelines_map={
        Events.PERSON_ADD_MIDDLE_NAME.value: person_mid_name_pipeline,
        Events.PERSON_MULTIPLY_AGE.value: person_age_pipeline
    },
    max_concurrent_pipelines=256,
    logger=LOGGER
)

app = KafkaApp(config)

if __name__ == "__main__":
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        time.sleep(1)
        event_cache_client.close()
