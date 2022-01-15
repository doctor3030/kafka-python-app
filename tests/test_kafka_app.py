import sys
import asyncio
import signal
import json
from typing import Dict, Union, List, Optional, Any
import pydantic

sys.path.append('/home/doc/Documents/Projects/Rehoboam')
# from utils.logging import Logger
from kafka_connector import ListenerConfig, KafkaConnector, ConsumerRecord
# from utils.graceful_killer import GracefulKiller

# import api


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
            _logger_cls = Logger()
            self.logger = _logger_cls.default_logger
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

if __name__ == '__main__':
    from api.web.main import Message as WebMessage
    # from api.internal.main import Message as InternalMessage
    # from api.web.objects.user import UserCommands
    # from api.web.objects.user import UserCreateCommand
    # from api.web.objects.crawler import CrawlerCreateCommand

    KAFKA_BOOTSTRAP_SERVERS = ['10.0.0.74:9092']
    TEST_TOPIC = 'test_topic'

    killer = GracefulKiller()
    signal.signal(signal.SIGINT, killer.exit_gracefully)
    signal.signal(signal.SIGTERM, killer.exit_gracefully)

    kafka_config = KafkaConfig(**{
        'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
        'listen_topics': [TEST_TOPIC],
        'message_cls': WebMessage
    })
    config = AppConfig(**{
        'kafka_config': kafka_config,
    })

    producer = KafkaConnector.get_producer(KAFKA_BOOTSTRAP_SERVERS)
    app = KafkaApp(config)
    killer.obj = app

    @app.on_command(api.web.objects.user.UserCreateCommand)
    def handle_command(message):
        print('Handling UserCreateCommand..')
        print('Received: {}'.format(message))
        app.msg_counter += 1


    @app.on_command(api.web.objects.crawler.CrawlerCreateCommand)
    def handle_command(message):
        print('Handling CrawlerCreateCommand..')
        print('Received: {}'.format(message))
        app.msg_counter += 1


    msg_user_create = {
        'command': {
            'object': 'user',
            'action': 'create'
        },
        'params': {
            'username': 'doctor3030',
            'password': 'pass',
            'email': 'aaa@aaa.com'
        }
    }

    msg_crawler_create = {
        'command': {
            'object': 'crawler',
            'action': 'create'
        }
    }

    msg = WebMessage(**msg_user_create)
    producer.send(TEST_TOPIC, json.loads(msg.json(exclude_unset=True)))

    msg = WebMessage(**msg_crawler_create)
    producer.send(TEST_TOPIC, json.loads(msg.json(exclude_unset=True)))

    import time
    s = time.perf_counter()
    asyncio.run(app.run())
    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")
