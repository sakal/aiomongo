from collections import deque

from bson.son import SON
from pymongo import helpers
from pymongo.errors import OperationFailure
from pymongo.message import _GetMore

import aiomongo


class CommandCursor:
    """A cursor / iterator over command cursors.
    """

    def __init__(self, connection: 'aiomongo.Connection', collection: 'aiomongo.Collection',
                 cursor_info: dict, retrieved: int = 0):
        """Create a new command cursor.
        """
        self.__connection = connection
        self.__collection = collection
        self.__id = cursor_info['id']
        self.__data = deque(cursor_info['firstBatch'])
        self.__retrieved = retrieved
        self.__batch_size = 0
        self.__killed = (self.__id == 0)

        if 'ns' in cursor_info:
            self.__ns = cursor_info['ns']
        else:
            self.__ns = str(collection)

    async def close(self) -> None:
        """Explicitly close / kill this cursor. Required for PyPy, Jython and
        other Python implementations that don't use reference counting
        garbage collection.
        """
        if not self.__killed:
            self.__killed = True
            if self.__id:
                spec = SON([('killCursors', self.__collection.name),
                            ('cursors', [self.__id])])
                await self.__connection.command(
                    self.__collection.database.name, spec, self.__collection.read_preference,
                    self.__collection.codec_options
                )

    def batch_size(self, batch_size: int) -> 'CommandCursor':
        """Limits the number of documents returned in one batch. Each batch
        requires a round trip to the server. It can be adjusted to optimize
        performance and limit data transfer.

        .. note:: batch_size can not override MongoDB's internal limits on the
           amount of data it will return to the client in a single batch (i.e
           if you set batch size to 1,000,000,000, MongoDB will currently only
           return 4-16MB of results per batch).

        Raises :exc:`TypeError` if `batch_size` is not an integer.
        Raises :exc:`ValueError` if `batch_size` is less than ``0``.

        :Parameters:
          - `batch_size`: The size of each batch of results requested.
        """
        if not isinstance(batch_size, int):
            raise TypeError('batch_size must be an integer')
        if batch_size < 0:
            raise ValueError('batch_size must be >= 0')

        self.__batch_size = batch_size == 1 and 2 or batch_size
        return self

    async def _refresh(self) -> None:
        """Refreshes the cursor with more data from the server.

        Returns the length of self.__data after refresh. Will exit early if
        self.__data is already non-empty. Raises OperationFailure when the
        cursor cannot be refreshed due to an error on the query.
        """
        if len(self.__data) or self.__killed:
            return len(self.__data)

        if self.__id:  # Get More
            dbname, collname = self.__ns.split('.', 1)

            try:
                data = await self.__connection.perform_operation(
                    _GetMore(dbname,
                             collname,
                             self.__batch_size,
                             self.__id,
                             self.__collection.codec_options))
            except EOFError:
                self.__killed = True
                raise

            try:
                doc = helpers._unpack_response(data,
                                               self.__id,
                                               self.__collection.codec_options)
                helpers._check_command_response(doc['data'][0])
            except OperationFailure:
                self.__killed = True

                raise

            cursor = doc['data'][0]['cursor']
            documents = cursor['nextBatch']
            self.__id = cursor['id']
            self.__retrieved += len(documents)

            if self.__id == 0:
                self.__killed = True
            self.__data = deque(documents)

        else:  # Cursor id is zero nothing else to return
            self.__killed = True

        return len(self.__data)

    @property
    def alive(self) -> bool:
        """Does this cursor have the potential to return more data?

        Even if :attr:`alive` is ``True``, :meth:`next` can raise
        :exc:`StopIteration`. Best to use a for loop::

            for doc in collection.aggregate(pipeline):
                print(doc)

        .. note:: :attr:`alive` can be True while iterating a cursor from
          a failed server. In this case :attr:`alive` will return False after
          :meth:`next` fails to retrieve the next batch of results from the
          server.
        """
        return bool(len(self.__data) or (not self.__killed))

    def __aiter__(self) -> 'CommandCursor':
        return self

    async def __anext__(self) -> dict:
        if len(self.__data):
            return self.__data.popleft()

        is_refreshed = await self._refresh()
        if not is_refreshed:
            raise StopAsyncIteration

        return self.__data.popleft()

    async def __aenter__(self) -> 'CommandCursor':
        return self

    async def __aexit__(self, *exc) ->None:
        await self.close()
