from time import sleep
from kafka import KafkaProducer
import json


class Generator:
    def __init__(self, broker_url, tps=10):
        self.broker_url = broker_url
        self.tps = tps
        self.sleep_time = 1 / tps
        self.producer = KafkaProducer(bootstrap_servers=broker_url,
                                      value_serializer=lambda value: json.dumps(value).encode())

    def send_message(self, topic, message):
        self.producer.send(topic, value=message)


# if __name__ == "__main__":
#     producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL,
#                              value_serializer=lambda value: json.dumps(value).encode())
#     while True:
#         claim: dict = create_random_claim()
#         producer.send(CLAIMS_TOPIC, value=claim)
#         print(claim)
#         sleep(SLEEP_TIME)
