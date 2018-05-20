# -*- coding: utf-8 -*-
import json
import traceback

from kafka.client import KafkaClient
from kafka.common import OffsetRequest
from kafka.util import kafka_bytestring
from kafka.consumer import MultiProcessConsumer
from kafka.producer import SimpleProducer
from kafka.consumer import SimpleConsumer
from kafka.common import BrokerResponseError
import log_util
import settings
from decorators import retry


class KafkaHelper(object):
    def __init__(self):
        self.client = None
        self.producer = None
        self.consumer = None
        self.consumer_fetch_timeout = None
        self.consumer_fetch_size = None

    def __enter__(self):
        self.get_client()
        return self

    def __exit__(self, exctype, excvalue, traceback):
        self.close_client()

    @retry(BrokerResponseError, tries=5, delay=3, backoff=2)
    def get_client(self):
        if not self.client:
            self.client = KafkaClient(settings.KAFKA['host'])
        return self.client

    def get_producer(self):
        """
        :return: SimpleProducer
        """
        if not self.producer:
            self.get_client()
            self.producer = SimpleProducer(self.client)
        return self.producer

    def get_multiprocess_consumer(self, consumer_group, topic, fetch_size=settings.KAFKA['message_fetch_batch'],
                     fetch_timeout=settings.KAFKA['message_fetch_timeout'],
                     auto_commit_every_n=settings.KAFKA['auto_commit_msg_count'], **kw):
        """
        Return MultiProcessConsumer which consumes partitions for a topic in
        parallel using multiple processes

        Arguments:
            consumer_group: a name for this consumer, used for offset storage and must be unique
            topic: the topic to consume

        Keyword Arguments:
            fetch_size: Indicates the maximum number of messages to be fetched
            fetch_timeout: The function will block for the specified
                time (in seconds) until count messages is fetched
            auto_commit_every_n: How many messages to consume
                before a commit
        """
        if not self.consumer:
            self.consumer_fetch_size = fetch_size
            self.consumer_fetch_timeout = fetch_timeout

            self.get_client()
            partitions = len(self.get_partitions(topic))
            self.consumer = MultiProcessConsumer(self.client, consumer_group, topic, num_procs=partitions,
                                                 partitions_per_proc=1, auto_commit_every_n=auto_commit_every_n, **kw)
        return self.consumer

    def get_consumer(self, consumer_group, topic, fetch_size=settings.KAFKA['message_fetch_batch'],
                     fetch_timeout=settings.KAFKA['message_fetch_timeout'],
                     auto_commit_every_n=settings.KAFKA['auto_commit_msg_count'], **kw):
        if not self.consumer:
            self.consumer_fetch_size = fetch_size
            self.consumer_fetch_timeout = fetch_timeout

            self.get_client()
            self.consumer = SimpleConsumer(self.client, consumer_group, topic,
                                           auto_commit_every_n=auto_commit_every_n, auto_offset_reset='smallest', **kw)
        return self.consumer

    def close_client(self):
        if self.client:
            self.client.close()

    def send_message(self, topic, msgs, logger=None):
        content = [(json.dumps(msg) if type(msg) is dict else msg) for msg in msgs]
        try:
            resp = self.producer.send_messages(topic, *content)
            return resp
        except Exception as e:
            if logger:
                logger.error('An error has occured in KafkaHelper.send_message(), please check errors:  %s',
                             traceback.format_exc())
            raise e

    def receive_messages(self):
        messages = self.consumer.get_messages(count=self.consumer_fetch_size, timeout=self.consumer_fetch_timeout)
        return messages

    def current_offset(self, topic, partition):
        offsets, = self.client.send_offset_request([OffsetRequest(kafka_bytestring(topic),
                                                                  partition, -1, 1)])
        return offsets.offsets[0]

    def consumer_offset(self, consumer_name, topic, partition):
        offsets, = self.client.send_offset_fetch_request(consumer_name, [OffsetRequest(kafka_bytestring(topic), partition, -1, 1)])
        return offsets[2]

    def get_total_lags(self, consumer_name, topic):
        lags = []
        lag = 0
        partitions = self.get_partitions(topic)
        for p in partitions:
            offset1 = self.consumer_offset(consumer_name, topic, p)
            offset2 = self.current_offset(topic, p)
            lag = (offset2-offset1)
            lags.append(lag)
            #print offset1,offset2,lag
        return sum(lags)

    def get_partitions(self, topic):
        return self.client.get_partition_ids_for_topic(topic)


def kafka_producer():
    class _KafkaProducerWrapper(object):
        def __init__(self):
            self.helper = KafkaHelper()
            self.msg_logger = log_util.get_logger(tag="kafka_msg")

        def __enter__(self):
            self.helper.get_producer()
            return self

        def __exit__(self, exctype, excvalue, traceback):
            self.helper.producer.stop()
            self.helper.close_client()

        def send_message(self, topic, msgs, logger=None):
            return self.helper.send_message(topic, msgs, logger)

        def send_image_message(self, mls_id, image_downloading_task):
            self.send_message(settings.KAFKA['topic']['QUEUE_IMAGES']+str(mls_id), [image_downloading_task])
            self.msg_logger.updateRecord(mls_id)
            kafka_message = json.dumps(image_downloading_task)
            self.msg_logger.info("type: [%s]; value: [%s]; kafka_message: [%s]; [sent]"%('images', image_downloading_task['mls_listing_id'], kafka_message))

    return _KafkaProducerWrapper()


def allow_send_message(topic_key, consumer_name="", threshold=None):
    total_lag = 0
    topic_setting = settings.KAFKA['topic'][topic_key]
    topic = topic_setting['topic']
    if not threshold:
        threshold = topic_setting.get('lag_threshold', 200000)
        #print topic,threshold,consumer_name
    if not consumer_name:
        consumer_name = settings.KAFKA['consumer'].get('cachelistener', 'cachelistener')
    with KafkaHelper() as h:
        total_lag = h.get_total_lags(consumer_name, topic)

    return total_lag < threshold


def kafka_consumer(consumer_group, topic, fetch_size=settings.KAFKA['message_fetch_batch'],
                   fetch_timeout=settings.KAFKA['message_fetch_timeout'],
                   auto_commit_every_n=settings.KAFKA['auto_commit_msg_count'],
                   **kw):
    """
    Return a wrapper of KafkaHelper and expose its receive_messages method

    Arguments:
        consumer_group: a name for this consumer, used for offset storage and must be unique
        topic: the topic to consume

    Keyword Arguments:
        fetch_size: Indicates the maximum number of messages to be fetched
        fetch_timeout: The function will block for the specified
            time (in seconds) until count messages is fetched
        auto_commit_every_n: How many messages to consume before a commit
    """
    class _KafkaConsumerWrapper(object):
        def __init__(self):
            self.helper = KafkaHelper()

        def __enter__(self):
            self.helper.get_consumer(consumer_group, topic, fetch_size, fetch_timeout,
                                     auto_commit_every_n=auto_commit_every_n, **kw)
            return self

        def __exit__(self, exctype, excvalue, traceback):
            if kw.get('auto_commit', True):
                self.helper.consumer.commit()

            self.helper.consumer.stop()
            self.helper.close_client()

        def receive_messages(self):
            messages = self.helper.receive_messages()
            return messages

        def commit(self):
            self.helper.consumer.commit()

    return _KafkaConsumerWrapper()

def kafka_multi_consumer(consumer_group, topic, fetch_size=settings.KAFKA['message_fetch_batch'],
                   fetch_timeout=settings.KAFKA['message_fetch_timeout'],
                   auto_commit_every_n=settings.KAFKA['auto_commit_msg_count'], **kw):
    """
    Return a wrapper of KafkaHelper and expose its receive_messages method

    Arguments:
        consumer_group: a name for this consumer, used for offset storage and must be unique
        topic: the topic to consume

    Keyword Arguments:
        fetch_size: Indicates the maximum number of messages to be fetched
        fetch_timeout: The function will block for the specified
            time (in seconds) until count messages is fetched
        auto_commit_every_n: How many messages to consume
            before a commit
    """
    class _KafkaConsumerWrapper(object):
        def __init__(self):
            self.helper = KafkaHelper()

        def __enter__(self):
            self.helper.get_multiprocess_consumer(consumer_group, topic, fetch_size, fetch_timeout,
                                     auto_commit_every_n=auto_commit_every_n, **kw)
            return self

        def __exit__(self, exctype, excvalue, traceback):
            if kw.get('auto_commit', True):
                self.helper.consumer.commit()

            self.helper.consumer.stop()
            self.helper.close_client()

        def receive_messages(self):
            messages = self.helper.receive_messages()
            return messages

        def commit(self):
            self.helper.consumer.commit()

    return _KafkaConsumerWrapper()
