import asyncio
import uuid

from kafka import KafkaProducer, KafkaConsumer, ConsumerRebalanceListener
from kafka.consumer.fetcher import ConsumerRecord
import pydantic
from typing import List, Dict, Optional, Callable, Any, Tuple, ByteString
import json
import sys
import logging


class ListenerConfig(pydantic.BaseModel):
    bootstrap_servers: List[str]
    process_message_cb: Callable[[ConsumerRecord], None]
    consumer_config: Optional[Dict]
    topics: List[str]
    logger: Optional[Any]


class ProducerRecord(pydantic.BaseModel):
    value: Any
    key: Optional[Any]
    headers: Optional[List[Tuple[str, bytes]]]
    partition: Optional[int]
    timestamp_ms: Optional[int]


class KafkaConnector:

    @staticmethod
    def get_producer(bootstrap_servers, producer_config: Dict = None):
        config = {
            'bootstrap_servers': bootstrap_servers,
            'key_serializer': lambda x: x.encode('utf-8') if x else x,
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
        self.stop = False
        self.config = config
        if not config.logger:
            self.logger = logging.getLogger()
        else:
            self.logger = config.logger

        config = {
            'bootstrap_servers': config.bootstrap_servers,
            'group_id': str(uuid.uuid4()),
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': False,
            'key_deserializer': lambda x: x.decode('utf-8') if x else x,
            'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
            'session_timeout_ms': 25000
        }
        if self.config.consumer_config:
            for key in self.config.consumer_config.keys():
                config[key] = self.config.consumer_config.get(key)

        self.consumer = KafkaConsumer(**config)
        self.consumer.subscribe(topics=self.config.topics)

    # def __del__(self):
    #     self.stop = True
    #     self.close()

    def close(self):
        # self.logger.info('Closing listener..')
        self.consumer.close()
        self.logger.info('Kafka listener closed.')

    async def listen(self):
        try:
            while True:
                message_batch = self.consumer.poll()
                for partition_batch in message_batch.values():
                    for message in partition_batch:
                        self.config.process_message_cb(message)
                self.consumer.commit()

                if self.stop:
                    # self.logger.info('Shutting down..')
                    break

                await asyncio.sleep(0.5)

        except Exception as e:
            self.logger.error('Exception: type: {} line#: {} msg: {}'.format(sys.exc_info()[0],
                                                                             sys.exc_info()[2].tb_lineno,
                                                                             str(e)))

        finally:
            self.close()
