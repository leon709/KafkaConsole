
from kafka_util import KafkaHelper
with KafkaHelper() as h:
        consumer = h.get_consumer('test', 'tp_test1')
        consumer.seek(-100, 2)

print 'done!'