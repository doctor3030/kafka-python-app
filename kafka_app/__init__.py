from __future__ import absolute_import

__title__ = 'kafka-app'
__author__ = 'Dmitry Amanov'
__copyright__ = 'Copyright 2022 Dmitry Amanov'

# Set default logging handler to avoid "No handler found" warnings.
import logging
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())

from kafka_app.app import KafkaConfig, AppConfig, KafkaApp
from kafka_app.kafka_connector import ListenerConfig, KafkaConnector, ConsumerRecord

__all__ = [
    'KafkaConfig', 'AppConfig', 'KafkaApp', 'ListenerConfig', 'KafkaConnector', 'ConsumerRecord',
]
