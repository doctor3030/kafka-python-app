import asyncio
import json
import signal
import sys
import time
import os
import shutil
import random
from unittest import IsolatedAsyncioTestCase

sys.path.append('../')
from kafka_python_app.connector import ListenerConfig, KafkaConnector, ConsumerRecord
from loguru_logger_lite import Logger


KILL = False


class GracefulKiller:
    obj = None

    def exit_gracefully(self, *args):
        global KILL
        KILL = True
        if self.obj:
            self.obj.close()


killer = GracefulKiller()
signal.signal(signal.SIGINT, killer.exit_gracefully)
signal.signal(signal.SIGTERM, killer.exit_gracefully)

msg_counter = 0
time_start = 0


def process_message(message: ConsumerRecord) -> None:
    global msg_counter
    msg = json.loads(message.value)
    print(f'Received: {msg}: n={msg_counter}: after: {round(time.time() - time_start, 3)}s')
    lines = [f'Test line {i}\n' for i in range(200000)]
    with open(f'temp_data/test_{msg_counter}.txt', 'w') as f:
        f.writelines(lines)

    time.sleep(msg['sleep_time']/1000)
    msg_counter += 1


async def process_message_async(message: ConsumerRecord) -> None:
    global msg_counter
    msg = json.loads(message.value)
    print(f'Received: {msg}: n={msg_counter}: after: {round(time.time() - time_start, 3)}s')
    lines = [f'Test line {i}\n' for i in range(200000)]
    with open(f'temp_data/test_{msg_counter}.txt', 'w') as f:
        f.writelines(lines)
    msg_counter += 1
    for i in range(10):
        await asyncio.sleep(msg['sleep_time']/1000)


class TestKafkaListener(IsolatedAsyncioTestCase):
    LOGGER = Logger.get_default_logger()
    KAFKA_BOOTSTRAP_SERVERS = ['127.0.0.1:9092']
    TEST_TOPIC = 'test_topic'

    kafka_listener_config = ListenerConfig(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        process_message_cb=process_message_async,
        consumer_config={
            'group_id': 'test_group',
            # "max_poll_records": 50
        },
        topics=[TEST_TOPIC],
        logger=LOGGER
    )

    producer = KafkaConnector.get_producer(
        KAFKA_BOOTSTRAP_SERVERS,
        # {'batch_size': 10}
    )
    listener = KafkaConnector.get_listener(kafka_listener_config)

    killer.obj = listener

    if os.path.exists('temp_data'):
        shutil.rmtree('temp_data')
    os.makedirs('temp_data')

    async def watch_counter(self):
        global time_start
        while True:
            if msg_counter == 100:
                self.LOGGER.info('Received {} message and now closing...'.format(msg_counter))
                self.listener.stop = True
                break
            if KILL:
                break
            await asyncio.sleep(1)
        print(f'Total time: {time.time() - time_start}')

    async def test_listen(self):
        global time_start
        time_start = time.time()
        for i in range(100):
            sleep_time = random.randint(100, 800)
            msg = json.dumps({'sleep_time': sleep_time})
            self.producer.send(self.TEST_TOPIC, msg)
        await asyncio.sleep(0.5)
        self.producer.close()
        await asyncio.gather(self.listener.listen(), self.watch_counter())
