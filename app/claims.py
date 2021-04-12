__author__ = 'Rohan Singh'
from random import choices, randint
from string import ascii_letters, digits
from faker import Faker
account_chars: str = digits + ascii_letters
fake = Faker()


def get_random_id(k=12) -> str:
    """Return a random id."""
    return "".join(choices(account_chars, k=k))


def _random_amount() -> float:
    """Return a random amount for the claim"""
    return randint(5000, 100000)


def create_random_claim() -> dict:
    """Create a fake, random transaction"""
    return {
        "account_id": get_random_id(),
        "name": fake.name(),
        "address": fake.address(),
        "claim_id": get_random_id(8),
        "insured_amount": _random_amount()
    }
