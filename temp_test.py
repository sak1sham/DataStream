from kafka import KafkaConsumer
import json

def get_kafka_connection(topic, kafka_group, kafka_server, KafkaPassword, KafkaUsername, enable_auto_commit = True):
        if "aws" in kafka_server:
            return KafkaConsumer(topic,
                                bootstrap_servers=kafka_server,
                                security_protocol='SASL_SSL',
                                sasl_mechanism='SCRAM-SHA-512',
                                sasl_plain_username=KafkaUsername,
                                sasl_plain_password=KafkaPassword,
                                enable_auto_commit=enable_auto_commit,
                                #  auto_offset_reset='earliest',
                                group_id=kafka_group,
                                value_deserializer=lambda m: json.loads(m.decode('utf-8')))
                                
consumer = get_kafka_connection(topic='audit_logs', kafka_group='dms_group', kafka_server='b-2.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096,b-3.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096,b-1.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096,b-4.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096,b-5.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096,b-6.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096', KafkaUsername='kafka-client-user', KafkaPassword='JjZXllrTsb6KgOVM', enable_auto_commit = True)

for msg in consumer:
    print("Consumer records:\n")
    print(msg.value)
    break
# print(consumer)

# from temp_ import a, b