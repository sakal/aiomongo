from collections import deque

from bson.codec_options import DEFAULT_CODEC_OPTIONS
from bson.son import SON
from pymongo import helpers
from pymongo.common import validate_is_mapping
from pymongo.cursor import _QUERY_OPTIONS
from pymongo.message import _GetMore, _Query
from pymongo.read_preferences import ReadPreference


class Cursor:

    def __init__(self, connection, collection, filter, projection, skip, limit, sort, modifiers,
                 batch_size=0):

        spec = filter
        if spec is None:
            spec = {}

        validate_is_mapping('filter', spec)
        if not isinstance(skip, int):
            raise TypeError('skip must be an instance of int')
        if not isinstance(limit, int):
            raise TypeError('limit must be an instance of int')

        if modifiers is not None:
            validate_is_mapping('modifiers', modifiers)

        if not isinstance(batch_size, int):
            raise TypeError('batch_size must be an integer')
        if batch_size < 0:
            raise ValueError('batch_size must be >= 0')

        if projection is not None:
            if not projection:
                projection = {'_id': 1}
            projection = helpers._fields_list_to_dict(projection, 'projection')

        self.__id = None
        self.__codec_options = DEFAULT_CODEC_OPTIONS
        self.__collection = collection
        self.__connection = connection
        self.__data = deque()
        self.__explain = False
        self.__max_scan = None
        self.__spec = spec
        self.__projection = projection
        self.__skip = skip
        self.__limit = limit
        self.__batch_size = batch_size
        self.__modifiers = modifiers or {}
        self.__ordering = sort and helpers._index_document(sort) or None
        self.__hint = None
        self.__comment = None
        self.__max_time_ms = None
        self.__max_await_time_ms = None
        self.__max = None
        self.__min = None
        self.__killed = False

        self.__codec_options = collection.codec_options
        self.__read_preference = collection.read_preference
        self.__read_concern = collection.read_concern
        self.__retrieved = 0

        self.__query_flags = 0
        if self.__read_preference != ReadPreference.PRIMARY:
            self.__query_flags |= _QUERY_OPTIONS['slave_okay']

    def __aiter__(self):
        return self

    def __query_spec(self):
        """Get the spec to use for a query.
        """
        operators = self.__modifiers
        if self.__ordering:
            operators['$orderby'] = self.__ordering
        if self.__explain:
            operators['$explain'] = True
        if self.__hint:
            operators['$hint'] = self.__hint
        if self.__comment:
            operators['$comment'] = self.__comment
        if self.__max_scan:
            operators['$maxScan'] = self.__max_scan
        if self.__max_time_ms is not None:
            operators['$maxTimeMS'] = self.__max_time_ms
        if self.__max:
            operators['$max'] = self.__max
        if self.__min:
            operators['$min'] = self.__min

        if operators:
            # Make a shallow copy so we can cleanly rewind or clone.
            spec = self.__spec.copy()

            # White-listed commands must be wrapped in $query.
            if '$query' not in spec:
                # $query has to come first
                spec = SON([('$query', spec)])

            if not isinstance(spec, SON):
                # Ensure the spec is SON. As order is important this will
                # ensure its set before merging in any extra operators.
                spec = SON(spec)

            spec.update(operators)
            return spec
        # Have to wrap with $query if 'query' is the first key.
        # We can't just use $query anytime 'query' is a key as
        # that breaks commands like count and find_and_modify.
        # Checking spec.keys()[0] covers the case that the spec
        # was passed as an instance of SON or OrderedDict.
        elif ('query' in self.__spec and
                  (len(self.__spec) == 1 or
                           next(iter(self.__spec)) == 'query')):
            return SON({'$query': self.__spec})

        return self.__spec

    async def __anext__(self) -> dict:

        if len(self.__data):
            return self.__data.popleft()

        is_refereshed = await self._refresh()

        if not is_refereshed:
            raise StopAsyncIteration

        return self.__data.popleft()

    async def _refresh(self) -> None:

        if len(self.__data) or self.__killed:
            return 0

        is_query = False
        if self.__id is None:
            is_query = True
            data = await self.__connection.perform_operation(
                _Query(self.__query_flags,
                       self.__collection.database.name,
                       self.__collection.name,
                       self.__skip,
                       self.__query_spec(),
                       self.__projection,
                       self.__codec_options,
                       self.__read_preference,
                       self.__limit,
                       self.__batch_size,
                       self.__read_concern)
            )
        elif self.__id:
            if self.__limit:
                limit = self.__limit - self.__retrieved
                if self.__batch_size:
                    limit = min(limit, self.__batch_size)
            else:
                limit = self.__batch_size

            try:
                data = await self.__connection.perform_operation(
                    _GetMore(self.__collection.database.name,
                             self.__collection.name,
                             limit,
                             self.__id,
                             self.__codec_options,
                             self.__max_await_time_ms)
                )
            except EOFError:
                self.__killed = True
                raise
        else:
            self.__killed = True
            self.__data = data = None

        if data:
            doc = helpers._unpack_response(response=data,
                                           cursor_id=self.__id,
                                           codec_options=self.__codec_options)

            helpers._check_command_response(doc['data'][0])

            cursor = doc['data'][0]['cursor']
            self.__id = cursor['id']

            if is_query:
                documents = cursor['firstBatch']
            else:
                documents = cursor['nextBatch']
            self.__data = deque(documents)

            self.__retrieved += len(documents)

        if self.__id == 0:
            self.__killed = True

        if self.__limit and self.__id and self.__limit <= self.__retrieved:
            await self.close()

        return len(self.__data)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def close(self) -> None:
        if not self.__killed:
            self.__killed = True
            if self.__id:
                spec = SON([('killCursors', self.__collection.name),
                            ('cursors', [self.__id])])
                await self.__connection.command(
                    self.__collection.database.name, spec, self.__read_preference,
                    self.__codec_options
                )


