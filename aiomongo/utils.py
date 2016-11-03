import asyncio
import random


class IncrementalSleeper:
    """ Helper class that allows to sleep with incrementally
        increasing time to be used in exponential backoff strategy
        when reconnect.
    """

    initial_delay = 0.4
    factor = 2.7182818284590451
    jitter = 0.11962656472

    def __init__(self, loop: asyncio.AbstractEventLoop, max_delay: float = 10.0):
        self.delay = self.initial_delay
        self.loop = loop
        self.max_delay = max_delay

    def reset(self):
        self.delay = self.initial_delay

    async def sleep(self) -> None:
        new_delay = min(self.delay * self.factor, self.max_delay)
        self.delay = random.normalvariate(new_delay, new_delay*self.jitter)

        await asyncio.sleep(self.delay, loop=self.loop)

