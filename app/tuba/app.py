import json
import os

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

load_dotenv()

KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL')
CLAIMS_TOPIC = os.getenv('CLAIMS_TOPIC')
LEGIT_CLAIMS_TOPIC = os.getenv('LEGIT_CLAIMS_TOPIC')
FRAUD_CLAIMS_TOPIC = os.getenv('FRAUD_CLAIMS_TOPIC')


def is_suspicious(claim_: dict) -> bool:
    """Return if a transactions in suspicious"""
    return True \
        if "doe" in claim_["name"] or claim_["insured_amount"] > 60000 \
        else False


if __name__ == "__main__":
    consumer = KafkaConsumer(
        CLAIMS_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda value: json.loads(value),
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    for message in consumer:
        claim: dict = message.value
        topic = FRAUD_CLAIMS_TOPIC if is_suspicious(claim) else LEGIT_CLAIMS_TOPIC
        producer.send(topic, value=claim)
        print(topic, claim)
