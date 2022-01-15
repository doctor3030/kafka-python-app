import sys
import asyncio
from typing import Dict, List, Optional, Any
import pydantic
import logging

sys.path.append('/home/doc/Documents/Projects/kafka-app')
from kafka_connector import ListenerConfig, KafkaConnector, ConsumerRecord


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
            'topics': config.kafka_config.listen_topics,
            'logger': self.logger
        })
        self.listener = KafkaConnector.get_listener(kafka_listener_config)

        self._command_map: Dict = {}
        self.msg_counter = 0

    def process_message(self, message: ConsumerRecord) -> None:
        _value = message.value
        _message = self.config.kafka_config.message_cls(**_value)
        handle = self._command_map.get(type(_message.command))
        if handle:
            handle(_message)

    async def watch_counter(self):
        while True:
            if self.msg_counter > 0 or self.KILL_PROCESS:
                self.logger.info('Received {} message and now closing...'.format(self.msg_counter))
                self.listener.KILL_PROCESS = True
                break
            await asyncio.sleep(0.1)

    def on_command(self, command):

        def decorator(func):
            self._command_map[command] = func

            def wrapper(*args, **kwargs):
                func(*args, **kwargs)
            return wrapper

        return decorator

    async def run(self):
        await asyncio.gather(self.listener.listen(), self.watch_counter())

    def close(self):
        self.listener.KILL_PROCESS = True
        self.producer.close()
        self.logger.close()

