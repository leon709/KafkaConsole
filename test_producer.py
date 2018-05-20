# -*- coding: utf-8 -*-
import traceback
import json
import time
from kafka.client import KafkaClient
from kafka.common import OffsetRequest
from kafka.util import kafka_bytestring
from kafka.consumer import MultiProcessConsumer
from kafka.producer import SimpleProducer


def send_msg(msgs):
    cli = KafkaClient("localhost:9092")
    producer = SimpleProducer(cli)
    if isinstance(msgs, list):
        content = [(json.dumps(msg) if isinstance(msg, dict) else msg) for msg in msgs]
    else:
        content = [msgs]
    try:
        resp = producer.send_messages("tp_test1", *content)
        print resp
    except Exception:
        print traceback.format_exc()
    finally:
        cli.close()


if __name__ == '__main__':
    while True:
        send_msg(["hello leon", "last", "time"])
        time.sleep(5)
