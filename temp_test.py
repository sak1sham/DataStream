from helper.util import *
from db.kafka_dms import *


consumer = get_kafka_connection(topic='audit_logs', kafka_group='dms_group', kafka_server='b-2.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096,b-3.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096,b-1.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096,b-4.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096,b-5.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096,b-6.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096', KafkaUsername='kafka-client-user', KafkaPassword='JjZXllrTsb6KgOVM', enable_auto_commit = True)

for msg in consumer:
    print("Consumer records:\n")
    print(msg.value)
    break
# print(consumer)