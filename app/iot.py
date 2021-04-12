__author__ = 'Rohan Singh'
from random import choices, randint
from string import ascii_letters, digits
from faker import Faker
import random

account_chars: str = digits + ascii_letters
fake = Faker()


def get_random_id(k=12) -> str:
    """Return a random id."""
    return "".join(choices(account_chars, k=k))


def _random_amount() -> float:
    """Return a random amount for the claim"""
    return randint(5000, 100000)


def random_telematics(customers=10, records=10) -> list:
    """Create a fake, random transaction"""
    names = [fake.unique.name() for _ in range(customers)]

    return [{random.choice(names): random.gauss(60, 20)}
            for _ in range(records * customers)]
