import inspect
import json
import sys
import uuid
from typing import Dict, List, Optional, Any, Callable, Union, Coroutine, Tuple
import pydantic
import logging
import hashlib
import time
import asyncio
from collections import deque
from kafka_python_app.connector import ListenerConfig, KafkaConnector, ConsumerRecord, ProducerRecord


def _get_event_id_hash(event_id: str, service_id: str):
    _id = '.'.join([service_id, event_id])
    return hashlib.sha256(_id.encode('utf-8')).hexdigest()


class TransactionPipeWithReturnOptions(pydantic.BaseModel):
    response_event_name: str
    response_from_topic: str
    cache_client: Any
    return_event_timeout: int


class TransactionPipeResultOptions(pydantic.BaseModel):
    pipe_event_name: str
    pipe_to_topic: str
    with_response_options: Optional[TransactionPipeWithReturnOptions]


class MessageTransaction(pydantic.BaseModel):
    fnc: Callable
    args: Optional[Dict]
    pipe_result_options: Optional[TransactionPipeResultOptions]


class MessagePipeline(pydantic.BaseModel):
    transactions: List[MessageTransaction]
    app_id: Optional[str]
    logger: Optional[Any]

    __exceptions: List[Any] = []

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not kwargs.get('logger'):
            logging.basicConfig()
            self.logger = logging.getLogger()
            self.logger.setLevel(logging.INFO)

    async def execute(self, message, emitter, message_key_as_event, **kwargs):
        try:
            for tx in self.transactions:
                self.logger.debug(f'Executing transaction: '
                                  f'name: {getattr(tx.fnc, "__name__", repr(tx.fnc))}; '
                                  f'args: {tx.args}')
                if inspect.iscoroutinefunction(tx.fnc):
                    if tx.args:
                        message = await tx.fnc(message, self.logger, **{**tx.args, **kwargs})
                    else:
                        message = await tx.fnc(message, self.logger, **kwargs)
                else:
                    if tx.args:
                        message = tx.fnc(message, self.logger, **{**tx.args, **kwargs})
                    else:
                        message = tx.fnc(message, self.logger, **kwargs)

                if tx.pipe_result_options:
                    if tx.pipe_result_options.with_response_options:
                        message = await self.pipe_event_with_response(message, emitter, message_key_as_event,
                                                                      tx.pipe_result_options, **kwargs)
                    else:
                        self.pipe_event(message, emitter, message_key_as_event, tx.pipe_result_options, **kwargs)
        except Exception as e:
            self.logger.error(f'execute => Exception: '
                              f'type: {sys.exc_info()[0]}; '
                              f'line#: {sys.exc_info()[2].tb_lineno}; '
                              f'msg: {str(e)}')

    def pipe_event(
            self,
            message,
            emitter,
            message_key_as_event,
            options: TransactionPipeResultOptions,
            **kwargs
    ):
        event_id = None
        headers = kwargs.get('headers')
        if headers:
            headers = dict((x, y.decode('utf-8')) for x, y in kwargs.get('headers'))
            event_id = headers.get('event_id')
        if not event_id:
            event_id = str(uuid.uuid4())
        if headers:
            headers['event_id'] = event_id.encode('utf-8')
            headers = list(headers.items())
        else:
            headers = [('event_id', event_id.encode('utf-8'))]
        try:
            self.logger.info(
                f'<-------- PIPING EVENT: '
                f'name: {options.pipe_event_name}; '
                f'to: {options.pipe_to_topic}; '
                f'event_id: {event_id}')

            if not message_key_as_event:
                message['event'] = options.pipe_event_name
                emitter(
                    options.pipe_to_topic,
                    ProducerRecord(value=json.dumps(message), headers=headers)
                )
            else:
                emitter(
                    options.pipe_to_topic,
                    ProducerRecord(key=options.pipe_event_name, value=json.dumps(message),
                                   headers=[('event_id', event_id.encode('utf-8'))])
                )
            return event_id
        except Exception as e:
            self.logger.error(f'pipe_event => Exception: '
                              f'type: {sys.exc_info()[0]}; '
                              f'line#: {sys.exc_info()[2].tb_lineno}; '
                              f'msg: {str(e)}')
            return None

    async def pipe_event_with_response(
            self,
            message,
            emitter,
            message_key_as_event,
            options: TransactionPipeResultOptions,
            **kwargs
    ):
        cache_client = options.with_response_options.cache_client
        event_id = self.pipe_event(message, emitter, message_key_as_event, options)

        if not event_id:
            raise RuntimeError('Pipe event failed')

        time_up = time.time()
        while True:
            response = cache_client.get(_get_event_id_hash(event_id, self.app_id))
            if response is not None:
                response_msg = json.loads(response.decode('utf-8'))
                self.logger.info(
                    f'--------> PIPE RESPONSE RECEIVED: '
                    f'name: {options.with_response_options.response_event_name}; '
                    f'event_id: {event_id}')
                return response_msg
            else:
                if time.time() - time_up < options.with_response_options.return_event_timeout:
                    await asyncio.sleep(0.001)
                else:
                    raise TimeoutError(f'Pipe event with response timeout: '
                                       f'piped event: {options.pipe_event_name}; '
                                       f'to: {options.pipe_to_topic}; '
                                       f'response event: {options.with_response_options.response_event_name}; '
                                       f'from: {options.with_response_options.response_from_topic} '
                                       f'event_id: {event_id}')


class EmitWithResponseOptions(pydantic.BaseModel):
    topic_event_list: List[Tuple[str, str]]
    cache_client: Any
    return_event_timeout: int


class AppConfig(pydantic.BaseModel):
    app_name: Optional[str]
    app_id: Optional[str]
    bootstrap_servers: List[str]
    producer_config: Optional[Dict]
    consumer_config: Optional[Dict]
    listen_topics: List[str]
    message_key_as_event: Optional[bool]
    message_value_cls: Optional[Dict[str, Any]]
    middleware_message_cb: Optional[Union[
        Callable[[ConsumerRecord], None],
        Callable[[ConsumerRecord], Coroutine[Any, Any, None]]
    ]]
    emit_with_response_options: Optional[EmitWithResponseOptions]
    pipelines_map: Optional[Dict[str, MessagePipeline]]
    max_concurrent_tasks: Optional[int]
    max_concurrent_pipelines: Optional[int]
    logger: Optional[Any]


class KafkaApp:

    def __init__(self, config: AppConfig):
        self.config = config
        if not self.config.logger:
            logging.basicConfig()
            self.logger = logging.getLogger()
            self.logger.setLevel(logging.INFO)
        else:
            self.logger = self.config.logger
        if self.config.app_name:
            self.app_name = self.config.app_name
        else:
            self.app_name = 'Kafka application'
        self.producer = KafkaConnector.get_producer(config.bootstrap_servers,
                                                    config.producer_config)
        if self.config.app_id:
            self.app_id = self.config.app_id
        else:
            self.app_id = str(uuid.uuid4())

        kafka_listener_config = ListenerConfig(
            bootstrap_servers=config.bootstrap_servers,
            process_message_cb=self._process_message,
            consumer_config=config.consumer_config,
            topics=config.listen_topics,
            logger=self.logger
        )

        self.listener = KafkaConnector.get_listener(kafka_listener_config)

        self.event_map: Dict = {}
        self.pipelines_map = {}

        if self.config.pipelines_map:
            self.pipelines_map = self.config.pipelines_map
            caching_pipelines_map = {}
            for pipeline in self.pipelines_map.values():
                pipeline.app_id = self.app_id
                for txn in pipeline.transactions:
                    if txn.pipe_result_options and txn.pipe_result_options.with_response_options:
                        self._register_caching_pipeline(
                            (
                                txn.pipe_result_options.with_response_options.response_from_topic,
                                txn.pipe_result_options.with_response_options.response_event_name
                            ),
                            txn.pipe_result_options.with_response_options.cache_client,
                            caching_pipelines_map
                        )
                self.pipelines_map = {**self.pipelines_map, **caching_pipelines_map}
        else:
            self.pipelines_map: Dict[str, MessagePipeline] = {}

        if self.config.emit_with_response_options:
            for topic, event in self.config.emit_with_response_options.topic_event_list:
                self._register_caching_pipeline(
                    (topic, event),
                    self.config.emit_with_response_options.cache_client,
                    self.pipelines_map
                )

        self.sync_tasks_queue = deque()
        self.async_tasks_queue = deque()
        self.pipelines_queue = deque()
        self.caching_queue = deque()

        if self.config.max_concurrent_tasks:
            self.max_concurrent_tasks = self.config.max_concurrent_tasks
        else:
            self.max_concurrent_tasks = 100
        if self.config.max_concurrent_pipelines:
            self.max_concurrent_pipelines = self.config.max_concurrent_pipelines
        else:
            self.max_concurrent_pipelines = 100

        self.stop = False

    def _register_caching_pipeline(
            self,
            topic_event: Tuple[str, str],
            cache_client: Any,
            caching_pipelines_map: Dict[str, MessagePipeline]
    ):
        topic, event = topic_event
        key = '.'.join([topic, event])
        caching_pipelines_map[key] = MessagePipeline(
            transactions=[
                MessageTransaction(
                    fnc=self._cache_pipe_response,
                    args={
                        "cache_client": cache_client,
                    }
                )
            ],
            logger=self.logger,
            app_id=self.app_id
        )

    async def _process_message(self, message: ConsumerRecord) -> None:
        try:
            if self.config.middleware_message_cb:
                if not inspect.isfunction(self.config.middleware_message_cb):
                    raise ValueError('middleware_message_cb must be a function')

                if inspect.iscoroutinefunction(self.config.middleware_message_cb):
                    await self.config.middleware_message_cb(message)
                else:
                    self.config.middleware_message_cb(message)

            _message_value = message.value
            pipeline = None

            # Check if message.key to be used as event name
            if self.config.message_key_as_event:
                if self.pipelines_map.get(message.key):
                    pipeline = self.pipelines_map.get(message.key)
                elif self.pipelines_map.get('.'.join([message.topic, message.key])):
                    pipeline = self.pipelines_map.get('.'.join([message.topic, message.key]))

                handle = self.event_map.get('.'.join([message.topic, message.key]))
            else:
                assert _message_value.get('event') is not None, \
                    '"event" property is missing in message.value object. ' \
                    'Provide "event" property or set "message_key_as_event" option to True ' \
                    'to use message.key as event name.'

                if self.pipelines_map.get(_message_value.get('event')):
                    pipeline = self.pipelines_map.get(_message_value.get('event'))
                elif self.pipelines_map.get('.'.join([message.topic, _message_value.get('event')])):
                    pipeline = self.pipelines_map.get('.'.join([message.topic, _message_value.get('event')]))

                handle = self.event_map.get('.'.join([message.topic, _message_value.get('event')]))

            # Check if dataclass is provided for message.value
            if self.config.message_value_cls:
                _message_value_cls = self.config.message_value_cls[message.topic]
                _message = _message_value_cls(**_message_value)

            else:
                _message = _message_value

            kwargs = {
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "key": message.key,
                "headers": message.headers,
                "timestamp": message.timestamp,
                "timestamp_type": message.timestamp_type,
                "checksum": message.checksum,
                "serialized_key_size": message.serialized_key_size,
                "serialized_value_size": message.serialized_value_size,
                "serialized_header_size": message.serialized_header_size
            }

            if handle:
                if inspect.iscoroutinefunction(handle):
                    self.async_tasks_queue.append((handle, _message, kwargs))
                else:
                    self.sync_tasks_queue.append((handle, _message, kwargs))

            if pipeline:
                if getattr(pipeline.transactions[0].fnc, "__name__") == '_cache_pipe_response':
                    self.caching_queue.append((pipeline, _message_value, kwargs))
                else:
                    self.pipelines_queue.append((pipeline, _message_value, kwargs))

        except Exception as e:
            self.logger.error(f'_process_message => Exception: '
                              f'type: {sys.exc_info()[0]};'
                              f' line#: {sys.exc_info()[2].tb_lineno};'
                              f' msg: {str(e)}')

    async def _cache_pipe_response(
            self,
            message,
            logger,
            cache_client,
            **kwargs
    ):
        try:
            if self.config.message_key_as_event:
                event_name = kwargs.get('key')
            else:
                event_name = message["event"]
            headers = dict((x, y.decode('utf-8')) for x, y in kwargs.get('headers'))

            if logger:
                logger.info(
                    f'--------> CACHING PIPE RESPONSE: name: {event_name}; event_id: {headers["event_id"]}')

            cache_client.set(
                name=_get_event_id_hash(headers['event_id'], self.app_id),
                value=json.dumps(message).encode('utf-8'),
                ex=30
            )
            return message
        except Exception as e:
            err_msg = f'_cache_pipe_response => Exception: type: {sys.exc_info()[0]}; ' \
                      f'line#: {sys.exc_info()[2].tb_lineno}; ' \
                      f'msg: {str(e)}'
            self.logger.error(err_msg)

    def on(self, event: str, topic: Optional[str] = None):
        """
        Maps decorated function to topic.event key.

        If topic is provided, the decorated function is mapped to particular topic.event key that means an event that
        comes from a topic other than the specified will not be processed.
        Otherwise, event will be processed no matter which topic it comes from.
        :param event: Event name.
        :type event: str
        :param topic: Topic name
        :type topic: str
        :return:
        :rtype:
        """

        def decorator(func):
            if topic:
                self.event_map['.'.join([topic, event])] = func
            else:
                for t in self.config.listen_topics:
                    self.event_map['.'.join([t, event])] = func

            def wrapper(*args, **kwargs):
                func(*args, **kwargs)

            return wrapper

        return decorator

    def emit(self, topic: str, message: ProducerRecord):
        self.producer.send(topic,
                           message.value,
                           message.key,
                           message.headers,
                           message.partition,
                           message.timestamp_ms)

    async def emit_with_response(
            self,
            topic: str,
            message: ProducerRecord
    ):
        try:
            if not self.config.emit_with_response_options:
                raise AssertionError('Please provide emit_with_response_options in the application config.')

            event_id = str(uuid.uuid4())
            if message.headers:
                message.headers.append(('event_id', event_id.encode('utf-8')))
            else:
                message.headers = [('event_id', event_id.encode('utf-8'))]

            self.logger.info(
                f'<-------- PERFORMING EMIT WITH RESPONSE: '
                # f'event: {message.key if self.config.message_key_as_event else message.value["event"]}; '
                f'to: {topic}; '
                f'event_id: {event_id}')

            self.emit(topic, message)

            time_up = time.time()
            while True:
                response = self.config.emit_with_response_options.cache_client.get(_get_event_id_hash(event_id, self.app_id))
                if response is not None:
                    return json.loads(response.decode('utf-8'))
                else:
                    if time.time() - time_up < self.config.emit_with_response_options.return_event_timeout:
                        await asyncio.sleep(0.001)
                    else:
                        raise TimeoutError(f'Emit event with response timeout: '
                                           # f'emitted event: {message.key if self.config.message_key_as_event else message.value["event"]}; '
                                           f'to: {topic}; '
                                           f'event_id: {event_id}')
        except Exception as e:
            self.logger.error(f'txn_es_common => Exception: '
                              f'type: {sys.exc_info()[0]}; '
                              f'line#: {sys.exc_info()[2].tb_lineno}; '
                              f'msg: {str(e)}')

    async def process_sync_tasks(self):
        while True:
            if self.stop:
                if len(self.sync_tasks_queue) == 0:
                    break

            if len(self.sync_tasks_queue) > 0:
                handle, _message, kwargs = self.sync_tasks_queue.popleft()
                handle(_message, **kwargs)
            await asyncio.sleep(0.001)

    async def process_async_tasks(self):
        while True:
            if self.stop:
                if len(self.async_tasks_queue) == 0:
                    break

            if len(self.async_tasks_queue) > 0:
                batch_size = min(self.max_concurrent_tasks, len(self.async_tasks_queue))
                batch = [self.async_tasks_queue.popleft() for _ in range(batch_size)]
                tasks = [handle(_message, **kwargs) for handle, _message, kwargs in batch]
                await asyncio.gather(*tasks)
            await asyncio.sleep(0.001)

    async def process_pipelines(self):
        while True:
            if self.stop:
                if len(self.pipelines_queue) == 0:
                    break

            if len(self.pipelines_queue) > 0:
                batch_size = min(self.max_concurrent_pipelines, len(self.pipelines_queue))
                batch = [self.pipelines_queue.popleft() for _ in range(batch_size)]
                tasks = [
                    pipeline.execute(_message_value, self.emit, self.config.message_key_as_event, **kwargs)
                    for pipeline, _message_value, kwargs in batch
                ]
                await asyncio.gather(*tasks)
            await asyncio.sleep(0.001)

    async def process_caching(self):
        while True:
            if self.stop:
                if len(self.caching_queue) == 0:
                    break

            if len(self.caching_queue) > 0:
                pipeline, _message_value, kwargs = self.caching_queue.popleft()
                await pipeline.execute(_message_value, self.emit, self.config.message_key_as_event, **kwargs)
            await asyncio.sleep(0.001)

    async def run(self):
        self.logger.info('{} is up and running.'.format(self.app_name))
        try:
            await asyncio.gather(*[
                self.listener.listen(),
                self.process_sync_tasks(),
                self.process_async_tasks(),
                self.process_pipelines(),
                self.process_caching()
            ])
        except KeyboardInterrupt:
            self.close()

    def close(self):
        self.listener.stop = True
        self.stop = True
        self.producer.flush()
        self.producer.close()
        self.logger.info('{} closed.'.format(self.app_name))
