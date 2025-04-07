import random
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

order_schema = ""
schema_registry_client = SchemaRegistryClient({
    'url': "REGISTRY",
    'basic.auth.user.info': "TOKEN"
})

avro_serializer = AvroSerializer(schema_registry_client, order_schema)

string_serializer = StringSerializer('utf_8')

producer = Producer({
    ""
})


def message_generator(start_index: int = 0):
    code = ['408', '509', '201']
    for i in range(start_index, start_index + 1):
        yield {
            'id': f'ord_{i}',
            'customer_id': f'cus_{i}',
            'code': random.choice(code),
            'create_at': datetime.today().timestamp()
        }


def calc_partition(code: str):
    """ Get the partition, based on the key """
    if code == '408':
        return 0
    return 1

# TODO: set ack
if __name__ == '__main__':
    for msg in message_generator(1):
        # msg_gen = message_generator()
        #     msg = next(msg_gen)
        print(msg)

        producer.produce(topic="TOPIC",
                         key=string_serializer(msg['id']),
                         value=avro_serializer(msg, SerializationContext("TOPIC", MessageField.VALUE)),
                         partition=calc_partition(msg['code']))

        producer.flush()
        print("Message sent successfully")