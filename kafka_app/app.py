from typing import Dict, List, Optional, Any, Callable
import pydantic
import logging

from kafka_app.kafka_connector import ListenerConfig, KafkaConnector, ConsumerRecord, ProducerRecord


class KafkaConfig(pydantic.BaseModel):
    bootstrap_servers: List[str]
    producer_config: Optional[Dict]
    consumer_config: Optional[Dict]
    listen_topics: List[str]
    message_cls: Dict[str, Any]
    process_message_cb: Optional[Callable[[ConsumerRecord], None]]


class AppConfig(pydantic.BaseModel):
    kafka_config: KafkaConfig
    logger: Optional[Any]


class KafkaApp:

    def __init__(self, config: AppConfig):
        self.KILL_PROCESS = False
        self.config = config
        if not config.logger:
            self.logger = logging.getLogger()
        else:
            self.logger = config.logger

        self._producer = KafkaConnector.get_producer(config.kafka_config.bootstrap_servers,
                                                     config.kafka_config.producer_config)

        kafka_listener_config = ListenerConfig(**{
            'bootstrap_servers': config.kafka_config.bootstrap_servers,
            'process_message': self._process_message,
            'consumer_config': config.kafka_config.consumer_config,
            'topics': config.kafka_config.listen_topics,
            'logger': self.logger
        })
        self._listener = KafkaConnector.get_listener(kafka_listener_config)

        self._event_map: Dict = {}

    def _process_message(self, message: ConsumerRecord) -> None:
        _value = message.value
        _message = self.config.kafka_config.message_cls[message.topic](**_value)
        handle = self._event_map.get('.'.join([message.topic, _message.event]))

        if handle:
            # handle(_message)
            handle(_message, **{
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "timestamp": message.timestamp,
                "timestamp_type": message.timestamp_type,
            })

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
                self._event_map['.'.join([topic, event])] = func
            else:
                for t in self.config.kafka_config.listen_topics:
                    self._event_map['.'.join([t, event])] = func

            def wrapper(*args, **kwargs):
                func(*args, **kwargs)

            return wrapper

        return decorator

    def emit(self, topic: str, message: ProducerRecord):
        self._producer.send(topic,
                            message.value,
                            message.key,
                            message.headers,
                            message.partition,
                            message.timestamp_ms)

    async def run(self):
        await self._listener.listen()

    def close(self):
        self._listener.KILL_PROCESS = True
        self._producer.flush()
        self._producer.close()
