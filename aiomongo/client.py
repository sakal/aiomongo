import asyncio

from pymongo.client_options import ClientOptions
from pymongo.uri_parser import parse_uri

from .connection import Connection
from .database import Database


class AioMongoClient:

    _index = 0

    def __init__(self, uri: str, loop: asyncio.AbstractEventLoop):
        uri_info = parse_uri(uri=uri)
        assert len(uri_info['nodelist']) == 1, 'Can only connect to single node - either mongod or mongos'
        self.host = uri_info['nodelist'][0][0]
        self.port = uri_info['nodelist'][0][1]
        self.options = ClientOptions(
            uri_info['username'], uri_info['password'], uri_info['database'], uri_info['options']
        )
        self.loop = loop
        self._pool = []

    async def connect(self) -> None:
        self._pool = await asyncio.gather(
            *[Connection.create(
                self.loop, self.host, self.port, self.options.read_preference
            ) for _ in range(self.options.pool_options.max_pool_size)]
        )

    def __getitem__(self, item: str) -> Database:
        return Database(self, item)

    def __getattr__(self, item: str) -> Database:
        return self.__getitem__(item)

    def get_connection(self) -> Connection:
        # Get the next protocol available for communication in the pool.
        connection = self._pool[self._index]
        self._index = (self._index + 1) % len(self._pool)

        return connection

    def close(self) -> None:
        for conn in self._pool:
            conn.close()
