__author__ = 'Rohan Singh'
from fastapi import FastAPI
import os
from dotenv import load_dotenv
from generator import Generator
from pydantic import BaseModel
from typing import Optional
from claims import get_random_id, create_random_claim
from iot import random_telematics
from time import sleep

load_dotenv()


class Claim(BaseModel):
    account_id: str
    name: str
    address: str
    claim_id: Optional[str]
    insured_amount: float


app = FastAPI()

KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL')
CLAIMS_TOPIC = os.getenv('CLAIMS_TOPIC')
TELEMATICS_TOPIC = os.getenv("TELEMATICS_TOPIC")
LEGIT_CLAIMS_TOPIC = os.getenv('LEGIT_CLAIMS_TOPIC')
FRAUD_CLAIMS_TOPIC = os.getenv('FRAUD_CLAIMS_TOPIC')
TRANSACTIONS_PER_SECOND = float(os.getenv('TRANSACTIONS_PER_SECOND'))
SLEEP_TIME = 1 / TRANSACTIONS_PER_SECOND


@app.get("/")
async def welcome():
    return {"Hello, Hartford"}


@app.post("/create_claim")
async def create_claim(claim: Claim):
    generator = Generator(KAFKA_BROKER_URL, TRANSACTIONS_PER_SECOND)
    if claim.claim_id is None or len(claim.claim_id) == 0:
        claim.claim_id = get_random_id(k=10)
    print(claim)
    generator.send_message(CLAIMS_TOPIC, claim.dict())


@app.get("/bulk_create_claims/{quantity}")
async def bulk_create_claims(quantity: int):
    generator = Generator(KAFKA_BROKER_URL)

    for _ in range(quantity):
        claim: dict = create_random_claim()
        generator.send_message(CLAIMS_TOPIC, claim)


@app.get("/get_telematics_data")
async def get_telematics_data(customers: Optional[int] = 10):
    data = random_telematics(customers=customers)
    generator = Generator(KAFKA_BROKER_URL)
    for record in data:
        generator.send_message(TELEMATICS_TOPIC, record)
