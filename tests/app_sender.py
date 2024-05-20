import enum
import sys
import asyncio
import time
import json
from typing import Any, Dict
import pydantic
import redis
import random
from dateutil import parser
import uuid

sys.path.append('../')
from kafka_python_app.app import (AppConfig, KafkaApp, EmitWithResponseOptions, ProducerRecord,
                                  MessagePipeline, MessageTransaction, TransactionPipeResultOptions)
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

    PROCESS_COMPANY = 'process_company'
    COMPANY_ADD_INC = 'company_add_inc'
    COMPANY_DOUBLE_STOCK_PRICE = 'company_double_stock_price'
    COMPANY_PROCESSED = 'company_processed'

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


SERVICE_ID = str(uuid.uuid4())
SERVICE_NAME = 'APP SENDER'

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

# pipeline = MessagePipeline(
#     name='company_add_inc_pipeline',
#     transactions=[
#         MessageTransaction(
#             fnc=company_add_inc,
#             pipe_result_options=TransactionPipeResultOptions(
#                 pipe_event_name=Events.COMPANY_ADD_INC.value,
#                 pipe_to_topic=Topics.APP_1.value
#             )
#         ),
#     ],
#     logger=LOGGER
# )

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
        'group_id': 'test_app1_group',
        'auto_offset_reset': 'latest',
        'enable_auto_commit': False,
        'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
        'max_poll_records': 10,
        'session_timeout_ms': 25000
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
    logger=LOGGER
)

app = KafkaApp(config)


@app.on(Events.PERSON_ADD_MIDDLE_NAME_INIT.value)
async def handle_event(message: Dict, **kwargs):
    _time_up = time.time()
    LOGGER.info(f'message pipeline ({message.get("event")}) => STARTED')

    try:
        _msg = Message(**message)
        _msg.event = Events.PERSON_ADD_MIDDLE_NAME.value
        await app.emit_with_response(
            Topics.APP_2.value,
            ProducerRecord(value=_msg.model_dump_json(exclude_unset=True))
        )

    except Exception as e:
        err_msg = 'Exception: type: {} line#: {} msg: {}'.format(sys.exc_info()[0],
                                                                 sys.exc_info()[2].tb_lineno,
                                                                 str(e))
        LOGGER.error(err_msg)

    finally:
        _time_down = time.time()
        LOGGER.info(f'message pipeline ({message.get("event")}) => FINISHED '
                    f'| latency(s): {_time_down - _time_up}')


@app.on(Events.PERSON_MULTIPLY_AGE_INIT.value)
async def handle_event(message: Dict, **kwargs):
    _time_up = time.time()
    LOGGER.info(f'message pipeline ({message.get("event")}) => STARTED')

    try:
        _msg = Message(**message)
        _msg.event = Events.PERSON_MULTIPLY_AGE.value
        await app.emit_with_response(
            Topics.APP_2.value,
            ProducerRecord(value=_msg.model_dump_json(exclude_unset=True))
        )

    except Exception as e:
        err_msg = 'Exception: type: {} line#: {} msg: {}'.format(sys.exc_info()[0],
                                                                 sys.exc_info()[2].tb_lineno,
                                                                 str(e))
        LOGGER.error(err_msg)

    finally:
        _time_down = time.time()
        LOGGER.info(f'message pipeline ({message.get("event")}) => FINISHED '
                    f'| latency(s): {_time_down - _time_up}')


@app.on(Events.COMPANY_ADD_INC_INIT.value)
async def handle_event(message: Dict, **kwargs):
    _time_up = time.time()
    LOGGER.info(f'message pipeline ({message.get("event")}) => STARTED')

    try:
        _msg = Message(**message)
        _msg.event = Events.COMPANY_ADD_INC.value
        await app.emit_with_response(
            Topics.APP_3.value,
            ProducerRecord(value=_msg.model_dump_json(exclude_unset=True))
        )

    except Exception as e:
        err_msg = 'Exception: type: {} line#: {} msg: {}'.format(sys.exc_info()[0],
                                                                 sys.exc_info()[2].tb_lineno,
                                                                 str(e))
        LOGGER.error(err_msg)

    finally:
        _time_down = time.time()
        LOGGER.info(f'message pipeline ({message.get("event")}) => FINISHED '
                    f'| latency(s): {_time_down - _time_up}')


@app.on(Events.COMPANY_DOUBLE_STOCK_PRICE_INIT.value)
async def handle_event(message: Dict, **kwargs):
    _time_up = time.time()
    LOGGER.info(f'message pipeline ({message.get("event")}) => STARTED')

    try:
        _msg = Message(**message)
        _msg.event = Events.COMPANY_DOUBLE_STOCK_PRICE.value
        await app.emit_with_response(
            Topics.APP_3.value,
            ProducerRecord(value=_msg.model_dump_json(exclude_unset=True))
        )

    except Exception as e:
        err_msg = 'Exception: type: {} line#: {} msg: {}'.format(sys.exc_info()[0],
                                                                 sys.exc_info()[2].tb_lineno,
                                                                 str(e))
        LOGGER.error(err_msg)

    finally:
        _time_down = time.time()
        LOGGER.info(f'message pipeline ({message.get("event")}) => FINISHED '
                    f'| latency(s): {_time_down - _time_up}')

if __name__ == "__main__":
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        time.sleep(1)
        event_cache_client.close()
