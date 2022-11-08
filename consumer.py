from kafka import KafkaConsumer

bootstrap_server=["localhost:9092"]

topic="naturalnumber"

consumer=KafkaConsumer(bootstrap_servers = bootstrap_server)

for i in consumer:
    value=print(str(i.value.decode()))
    print(value)
    