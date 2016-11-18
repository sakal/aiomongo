import asyncio
from typing import Optional

from .client import AioMongoClient
from .collection import Collection
from .connection import Connection
from .database import Database


async def create_client(uri: str, loop: Optional[asyncio.AbstractEventLoop] = None) -> AioMongoClient:
    if loop is None:
        loop = asyncio.get_event_loop()
    cl = AioMongoClient(uri, loop)

    await cl.connect()

    return cl

__all__ = ['AioMongoClient', 'Collection', 'Connection', 'Database']
