import asyncio

from .client import AioMongoClient


async def create_client(uri: str, loop=None) -> AioMongoClient:
    if loop is None:
        loop = asyncio.get_event_loop()
    cl = AioMongoClient(uri, loop)

    await cl.connect()

    return cl

