import inspect
import sys
import asyncio
from typing import Dict, List, Optional, Any, Callable, Type
import pydantic
import logging
from kafka_python_app.connector import ListenerConfig, KafkaConnector, ConsumerRecord, ProducerRecord


class AppConfig(pydantic.BaseModel):
    app_name: Optional[str]
    bootstrap_servers: List[str]
    producer_config: Optional[Dict]
    consumer_config: Optional[Dict]
    listen_topics: List[str]
    message_key_as_event: Optional[bool]
    message_value_cls: Optional[Dict[str, Any]]
    middleware_message_cb: Optional[Callable[[ConsumerRecord], None]]
    logger: Optional[Any]


class KafkaApp:

    def __init__(self, config: AppConfig):
        self.config = config
        if not config.logger:
            self.logger = logging.getLogger()
        else:
            self.logger = config.logger
        if self.config.app_name:
            self.app_name = self.config.app_name
        else:
            self.app_name = 'Kafka application'
        self.producer = KafkaConnector.get_producer(config.bootstrap_servers,
                                                    config.producer_config)

        kafka_listener_config = ListenerConfig(
            bootstrap_servers=config.bootstrap_servers,
            process_message_cb=self._process_message,
            consumer_config=config.consumer_config,
            topics=config.listen_topics,
            logger=self.logger
        )

        self.listener = KafkaConnector.get_listener(kafka_listener_config)

        self.event_map: Dict = {}

    def __del__(self):
        self.close()

    def _process_message(self, message: ConsumerRecord) -> None:
        try:
            if self.config.middleware_message_cb:
                self.config.middleware_message_cb(message)

            _message_value = message.value

            # Check if message.key to be used as event name
            if self.config.message_key_as_event:
                handle = self.event_map.get('.'.join([message.topic, message.key]))
            else:
                assert _message_value.get('event') is not None, \
                    '"event" property is missing in message.value object. ' \
                    'Provide "event" property or set "message_key_as_event" option to True ' \
                    'to use message.key as event name.'
                handle = self.event_map.get('.'.join([message.topic, _message_value.get('event')]))

            # Check if dataclass is provided for message.value
            if self.config.message_value_cls:
                _message_value_cls = self.config.message_value_cls[message.topic]
                _message = _message_value_cls(**_message_value)

            else:
                _message = _message_value

            # If handler exists for a given event name, check if it's a coroutine and execute
            if handle and inspect.isfunction(handle):
                kwargs = {
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "timestamp": message.timestamp,
                    "timestamp_type": message.timestamp_type,
                }
                if inspect.iscoroutinefunction(handle):
                    loop = asyncio.get_running_loop()
                    loop.create_task(
                        handle(_message, **kwargs)
                    )
                    # loop.close()

                else:
                    handle(_message, **kwargs)

        except Exception as e:
            self.logger.error('Exception: type: {} line#: {} msg: {}'.format(sys.exc_info()[0],
                                                                             sys.exc_info()[2].tb_lineno,
                                                                             str(e)))

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

    async def run(self):
        self.logger.info('{} is up and running.'.format(self.app_name))
        await self.listener.listen()

    def close(self):
        self.listener.stop = True
        self.producer.flush()
        self.producer.close()
        self.logger.info('{} closed.'.format(self.app_name))
