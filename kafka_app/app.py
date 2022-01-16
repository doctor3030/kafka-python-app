from typing import Dict, List, Optional, Any
import pydantic
import logging

from kafka_app.kafka_connector import ListenerConfig, KafkaConnector, ConsumerRecord


class KafkaConfig(pydantic.BaseModel):
    bootstrap_servers: List[str]
    producer_config: Optional[Dict]
    consumer_config: Optional[Dict]
    listen_topics: List[str]
    message_cls: Any


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

        self.producer = KafkaConnector.get_producer(config.kafka_config.bootstrap_servers,
                                                    config.kafka_config.producer_config)

        kafka_listener_config = ListenerConfig(**{
            'bootstrap_servers': config.kafka_config.bootstrap_servers,
            'process_message': self.process_message,
            'consumer_config': config.kafka_config.consumer_config,
            'topics': config.kafka_config.listen_topics,
            'logger': self.logger
        })
        self.listener = KafkaConnector.get_listener(kafka_listener_config)

        self._event_map: Dict = {}

    def process_message(self, message: ConsumerRecord) -> None:
        _value = message.value
        _message = self.config.kafka_config.message_cls(**_value)
        handle = self._event_map.get(_message.event)
        if handle:
            handle(_message)

    def on(self, event):

        def decorator(func):
            self._event_map[event] = func

            def wrapper(*args, **kwargs):
                func(*args, **kwargs)
            return wrapper

        return decorator

    async def run(self):
        await self.listener.listen()

    def close(self):
        self.listener.KILL_PROCESS = True
        self.producer.close()

