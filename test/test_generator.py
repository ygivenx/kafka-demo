__author__ = 'Rohan Singh'
from app.generator import Generator
from app import claims
import os
from dotenv import load_dotenv
from iot import random_telematics

load_dotenv()

KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL')
CLAIMS_TOPIC = os.getenv('CLAIMS_TOPIC')
TRANSACTIONS_PER_SECOND = float(os.getenv('TRANSACTIONS_PER_SECOND'))
SLEEP_TIME = 1 / TRANSACTIONS_PER_SECOND


def test_generator():
    generator = Generator(KAFKA_BROKER_URL)
    claim = claims.create_random_claim()
    generator.send_message(message=claim, topic=CLAIMS_TOPIC)


def test_random_telematics():
    print(random_telematics())