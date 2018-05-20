# -*- coding: utf-8 -*-
import traceback
from kafka.client import KafkaClient
from kafka.common import OffsetRequest
from kafka.util import kafka_bytestring
from kafka.consumer import MultiProcessConsumer
from kafka.producer import SimpleProducer
from kafka.consumer import SimpleConsumer
from kafka.common import BrokerResponseError

cli = KafkaClient("localhost:9092")
consumer = SimpleConsumer(cli, 'test', 'tp_test1', auto_commit_every_n=10)

try:
    no_msg_times = 0
    while 1:
        is_over = False
        messages = consumer.get_messages(count=5, timeout=3)
        if messages:
            for m in messages:
                #print m
                msg_value = m.message.value
                print msg_value
                if msg_value == 'over':
                    is_over = True
        else:
            print "no msg!"
            no_msg_times += 1
            
        if is_over:
            print "The show is over! bye..."
            break
        if no_msg_times >= 5:
            print "no more msg"
            break
except Exception:
    print traceback.format_exc()
    
finally:
    cli.close()
    print "kafka connection closed!"
