from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

avro_deserializer = AvroDeserializer(SchemaRegistryClient({
    'url': "__",
    'basic.auth.user.info': "__"
}))

consumer = Consumer({
    'bootstrap.servers': '__',
    'sasl.mechanism': '__',
    'security.protocol': '__',
    'sasl.username': '__',
    'sasl.password': '__',
    'group.id': 'YOUR_CONSUMER_GROUP1',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit':'false'
})
consumer.subscribe(["t24_ld_loans_and_deposits"])
print('START')
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    try:

        print(msg)
        print({
            'hearder': msg.headers(),
            'key': msg.key(),
            'partition': msg.partition(),
            'offset': msg.offset(),
            'latency': msg.latency(),
            'timestamp': msg.timestamp()
        })
        deserialized = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        if deserialized is not None:
            print("Key {}: Value{} \n".format(msg.key(), deserialized))

        consumer.commit()
    except Exception as e:
        print(e)
        # consumer.seek(msg.partition())
# break

consumer.close()