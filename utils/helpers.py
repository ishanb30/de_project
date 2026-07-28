
import time
import random

def delay_retry(i: int) -> None:
    jitter = random.randint(1, 6)
    time.sleep(2 ** (i + 1) + jitter)


