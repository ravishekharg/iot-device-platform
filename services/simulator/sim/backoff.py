"""Reconnect delays.

After an outage every device tries to reconnect. Without jitter they all retry at the same
moments (1 s, 2 s, 4 s ...), hammering the broker in synchronized waves and often knocking it
over again: a reconnect storm. "Full jitter" picks a random delay between 0 and the
exponential cap, which spreads retries evenly over time.
"""
import random


def exponential_delay(attempt: int, base: float = 1.0, cap: float = 60.0) -> float:
    """Exponential backoff without jitter: base * 2^attempt, capped."""
    return min(cap, base * (2 ** attempt))


def full_jitter_delay(attempt: int, base: float = 1.0, cap: float = 60.0, rng: random.Random | None = None) -> float:
    """AWS-style "full jitter": uniform between 0 and the exponential delay."""
    rng = rng or random
    return rng.uniform(0, exponential_delay(attempt, base, cap))
