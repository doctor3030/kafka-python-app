# A lightweight application for consistent handling of kafka events

This application was developed to be a universal api endpoint for microservices that process kafka events.

## Installation

```
pip install kafka-python-app
```

## Usage

#### Configure and create application instance.

Configuration parameters:

- **app_name [OPTIONAL]**: application name.
- **bootstrap_servers**: list of kafka bootstrap servers addresses 'host:port'.
- **producer_config [OPTIONAL]**: kafka producer configuration (
  see [kafka-python documentation](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html)).

```python
# Default producer config
producer_config = {
    'key_serializer': lambda x: x.encode('utf-8') if x else x,
    "value_serializer": lambda x: json.dumps(x).encode('utf-8')
}
```

- **consumer_config [OPTIONAL]**: kafka consumer configuration (
  see [kafka-python documentation](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html)).

```python
# Default consumer config
conf = {
    'group_id': str(uuid.uuid4()),
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': False,
    'key_deserializer': lambda x: x.decode('utf-8') if x else x,
    'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
    'session_timeout_ms': 25000
}
```

- **listen_topics**: list of subscribed topics.
- **message_key_as_event [OPTIONAL]**: set to True if using kafka message key as event name.

> **_NOTE_** When setting ***message_key_as_event*** to True,
> make sure to specify valid **key_deserializer** in **consumer_config**.

- **message_value_cls**: dictionary {"topic_name": dataclass_type} that maps message dataclass type to specific topic.
  The application uses this mapping to create specified dataclass from kafka message.value.
- **middleware_message_cb [OPTIONAL]**: if provided, this callback will be executed with raw kafka message argument
  before calling any handler.
- **logger [OPTIONAL]**: any logger with standard log methods. If not provided, the standard python logger is used.

```python
from kafka_python_app.app import AppConfig, KafkaApp


# Setup kafka message payload:
class MyMessage(pydantic.BaseModel):
    event: str
    prop1: str
    prop2: int


# This message type is specific for "test_topic3" only. 
class MyMessageSpecific(pydantic.BaseModel):
    event: str
    prop3: bool
    prop4: float


# Create application config
config = AppConfig(
    app_name='Test application',
    bootstrap_servers=['localhost:9092'],
    consumer_config={
        'group_id': 'test_app_group'
    },
    listen_topics=['test_topic1', 'test_topic2', 'test_topic3'],
    message_value_cls={
        'test_topic1': MyMessage,
        'test_topic2': MyMessage,
        'test_topic3': MyMessageSpecific
    }
)

# Create application
app = KafkaApp(config)
```

#### Create event handlers using **@app.on** decorator:

```python
# This handler will handle 'some_event' no matter which topic it comes from
@app.on(event='some_event')
def handle_some_event(message: MyMessage, **kwargs):
    print('Handling event: {}..'.format(message.event))
    print('prop1: {}\n'.format(message.prop1))
    print('prop2: {}\n'.format(message.prop2))


# This handler will handle 'another_event' from 'test_topic2' only.
# Events with this name but coming from another topic will be ignored.
@app.on(event='another_event', topic='test_topic2')
def handle_some_event(message: MyMessage, **kwargs):
    print('Handling event: {}..'.format(message.event))
    print('prop1: {}\n'.format(message.prop1))
    print('prop2: {}\n'.format(message.prop2))


# This handler will handle 'another_event' from 'test_topic3' only.
@app.on(event='another_event', topic='test_topic3')
def handle_some_event(message: MyMessageSpecific, **kwargs):
    print('Handling event: {}..'.format(message.event))
    print('prop3: {}\n'.format(message.prop3))
    print('prop4: {}\n'.format(message.prop4))
```

> **_NOTE:_** If **topic** argument is provided to **@app.on** decorator,
> the decorated function is mapped to particular topic.event key that means
> an event that comes from a topic other than the specified
> will not be processed. Otherwise, event will be processed
> no matter which topic it comes from.

Use **app.emit()** method to send messages to kafka:

```python
from kafka_python_app import ProducerRecord

msg = ProducerRecord(
  key='my_message_key',
  value='my_payload'
)

app.emit(topic='some_topic', message=msg)
```


#### Start application:

```python
if __name__ == "__main__":
    asyncio.run(app.run())
```

## Use standalone kafka connector

The **KafkaConnector** class has two factory methods:

- **get_producer** method returns kafka-python producer.
- **get_listener** method returns instance of the **KafkaListener** class that wraps up kafka-python consumer and **
  listen()** method that starts consumption loop.

Use **ListenerConfig** class to create listener configuration:

```python
from kafka_python_app.connector import ListenerConfig

kafka_listener_config = ListenerConfig(
    bootstrap_servers=['ip1:port1', 'ip2:port2', ...],
    process_message_cb=my_process_message_func,
    consumer_config={'group_id': 'test_group'},
    topics=['topic1', 'topic2', ...],
    logger=my_logger
)
```

- **bootstrap_servers**: list of kafka bootstrap servers addresses 'host:port'.
- **process_message_cb**: a function that will be called on each message. 

```python
from kafka.consumer.fetcher import ConsumerRecord

def my_process_message_func(message: ConsumerRecord):
    print('Received kafka message: key: {}, value: {}'.format(
      message.key,
      message.value
    ))
```

- **consumer_config [OPTIONAL]**: kafka consumer configuration (
  see [kafka-python documentation](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html)).
- **topics**: list of topics to listen from.
- **logger [OPTIONAL]**: any logger with standard log methods. If not provided, the standard python logger is used.