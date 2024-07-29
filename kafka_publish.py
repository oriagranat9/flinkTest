from time import sleep
from json import dumps
from kafka import KafkaProducer, KafkaConsumer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

for e in range(2):
    producer.send('numtest', value={
        "ori": 21,
        "maya": "20"
    })

producer.flush()

consumer = KafkaConsumer(
    'output',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    group_id='my-group',
    value_deserializer=lambda x: x.decode('utf-8'))
for i in consumer:
    consumer.commit()
    print(i)
