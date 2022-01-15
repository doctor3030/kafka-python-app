import asyncio
from kafka import KafkaProducer, KafkaConsumer, ConsumerRebalanceListener
from kafka.consumer.fetcher import ConsumerRecord
import pydantic
from typing import List, Dict, Optional, Callable, Any
import json
import sys
import logging


class ListenerConfig(pydantic.BaseModel):
    bootstrap_servers: List[str]
    process_message: Callable[[ConsumerRecord], None]
    consumer_config: Optional[Dict]
    topics: List[str]
    logger: Optional[Any]


class KafkaConnector:

    @staticmethod
    def get_producer(bootstrap_servers, producer_config: Dict = None):
        config = {
            'bootstrap_servers': bootstrap_servers,
            'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
        }
        if producer_config:
            for key in producer_config.keys():
                config[key] = producer_config.get(key)

        return KafkaProducer(**config)

    @staticmethod
    def get_listener(config: ListenerConfig):
        return KafkaListener(config)


# class MyConsumerRebalanceListener(ConsumerRebalanceListener):
#     def __init__(self, logger: loguru_logger = None):
#         super(MyConsumerRebalanceListener, self).__init__()
#         if logger is None:
#             _logger_cls = Logger()
#             self.logger = _logger_cls.default_logger
#         else:
#             self.logger = logger
#
#     def on_partitions_assigned(self, assigned):
#         self.logger.info('Partitions assigned: {}'.format(assigned))
#
#     def on_partitions_revoked(self, revoked):
#         self.logger.info('Partitions revoked: {}'.format(revoked))


class KafkaListener:

    def __init__(self, config: ListenerConfig):
        self.KILL_PROCESS = False
        self.config = config
        if not config.logger:
            self.logger = logging.getLogger()
        else:
            self.logger = config.logger

        config = {
            'bootstrap_servers': config.bootstrap_servers,
            'group_id': 'test_group',
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': False,
            'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
            'session_timeout_ms': 25000
        }
        if self.config.consumer_config:
            for key in self.config.consumer_config.keys():
                config[key] = self.config.consumer_config.get(key)

        self.consumer = KafkaConsumer(**config)
        self.consumer.subscribe(topics=self.config.topics)

    def close(self):
        self.logger.info('Closing consumer..')
        self.consumer.close()
        self.logger.info('Listener closed.')

    async def listen(self):
        try:
            while True:
                message_batch = self.consumer.poll()
                for partition_batch in message_batch.values():
                    for message in partition_batch:
                        self.config.process_message(message)
                self.consumer.commit()

                if self.KILL_PROCESS:
                    self.logger.info('Shutting down..')
                    break

                await asyncio.sleep(0.5)

        except Exception as e:
            self.logger.error('Exception: type: {} line#: {} msg: {}'.format(sys.exc_info()[0],
                                                                             sys.exc_info()[2].tb_lineno,
                                                                             str(e)))

        finally:
            self.close()
