import asyncio
import signal
import sys
from unittest import IsolatedAsyncioTestCase

sys.path.append('../')
from kafka_python_app.connector import ListenerConfig, KafkaConnector, ConsumerRecord
from loguru_logger_lite import Logger


class GracefulKiller:
    obj = None

    def exit_gracefully(self, *args):
        if self.obj is not None:
            self.obj.stop = True


killer = GracefulKiller()
signal.signal(signal.SIGINT, killer.exit_gracefully)
signal.signal(signal.SIGTERM, killer.exit_gracefully)

msg_counter = 0


def process_message(message: ConsumerRecord) -> None:
    global msg_counter
    print('Received: {}'.format(message.value))
    msg_counter += 1


class TestKafkaListener(IsolatedAsyncioTestCase):

    # _cls_logger = Logger()
    LOGGER = Logger.get_default_logger()
    # KAFKA_BOOTSTRAP_SERVERS = ['192.168.2.190:9092']
    KAFKA_BOOTSTRAP_SERVERS = ['10.0.0.74:9092']
    TEST_TOPIC = 'test_topic'

    kafka_listener_config = ListenerConfig(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        process_message_cb=process_message,
        consumer_config={'group_id': 'test_group'},
        topics=[TEST_TOPIC],
        logger=LOGGER
    )

    producer = KafkaConnector.get_producer(KAFKA_BOOTSTRAP_SERVERS)
    listener = KafkaConnector.get_listener(kafka_listener_config)

    killer.obj = listener

    async def watch_counter(self):
        while True:
            if msg_counter > 0:
                self.LOGGER.info('Received {} message and now closing...'.format(msg_counter))
                self.listener.stop = True
                break
            await asyncio.sleep(0.1)

    async def test_listen(self):
        self.producer.send(self.TEST_TOPIC, 'Hello!')
        await asyncio.sleep(0.5)
        self.producer.close()
        await asyncio.gather(self.listener.listen(), self.watch_counter())

