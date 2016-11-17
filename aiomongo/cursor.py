import copy
from collections import deque
from typing import List, MutableMapping, Optional, Union

from bson import RE_TYPE
from bson.code import Code
from bson.codec_options import DEFAULT_CODEC_OPTIONS
from bson.son import SON
from pymongo import helpers
from pymongo.common import validate_boolean, validate_is_mapping
from pymongo.errors import InvalidOperation
from pymongo.cursor import _QUERY_OPTIONS
from pymongo.message import _GetMore, _Query
from pymongo.read_preferences import ReadPreference

import aiomongo


class Cursor:

    def __init__(self, collection: 'aiomongo.Collection',
                 filter: Optional[dict] = None, projection: Optional[Union[dict, list]] = None,
                 skip: int = 0, limit: int = 0, sort: Optional[List[tuple]] = None,
                 modifiers: Optional[dict] = None, batch_size: int = 0, no_cursor_timeout: bool = False) -> None:

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
        self.__connection = None
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
        if no_cursor_timeout:
            self.__query_flags |= _QUERY_OPTIONS['no_timeout']

    def __aiter__(self) -> 'Cursor':
        return self

    def __query_spec(self) -> SON:
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

    def __check_okay_to_chain(self) -> None:
        """Check if it is okay to chain more options onto this cursor.
        """
        if self.__retrieved or self.__id is not None:
            raise InvalidOperation('cannot set options after executing query')

    def _clone(self, deepcopy: bool=True) -> 'Cursor':
        """Internal clone helper."""
        clone = self._clone_base()
        values_to_clone = ('spec', 'projection', 'skip', 'limit',
                           'max_time_ms', 'max_await_time_ms', 'comment',
                           'max', 'min', 'ordering', 'explain', 'hint',
                           'batch_size', 'max_scan', 'manipulate',
                           'query_flags', 'modifiers')
        data = dict((k, v) for k, v in self.__dict__.items()
                    if k.startswith('_Cursor__') and k[9:] in values_to_clone)
        if deepcopy:
            data = self._deepcopy(data)
        clone.__dict__.update(data)
        return clone

    def _clone_base(self) -> 'Cursor':
        """Creates an empty Cursor object for information to be copied into.
        """
        return Cursor(self.__collection)

    def _deepcopy(self, x, memo=None):
        """Deepcopy helper for the data dictionary or list.

        Regular expressions cannot be deep copied but as they are immutable we
        don't have to copy them when cloning.
        """
        if not hasattr(x, 'items'):
            y, is_list, iterator = [], True, enumerate(x)
        else:
            y, is_list, iterator = {}, False, x.items()

        if memo is None:
            memo = {}
        val_id = id(x)
        if val_id in memo:
            return memo.get(val_id)
        memo[val_id] = y

        for key, value in iterator:
            if isinstance(value, (dict, list)) and not isinstance(value, SON):
                value = self._deepcopy(value, memo)
            elif not isinstance(value, RE_TYPE):
                value = copy.deepcopy(value, memo)

            if is_list:
                y.append(value)
            else:
                if not isinstance(key, RE_TYPE):
                    key = copy.deepcopy(key, memo)
                y[key] = value
        return y

    async def __anext__(self) -> MutableMapping:

        if len(self.__data):
            return self.__data.popleft()

        is_refereshed = await self._refresh()

        if not is_refereshed:
            raise StopAsyncIteration

        return self.__data.popleft()

    async def _refresh(self) -> None:

        if len(self.__data) or self.__killed:
            return 0

        if self.__connection is None:
            self.__connection = await self.__collection.database.client.get_connection()

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

            if not self.__explain:
                cursor = doc['data'][0]['cursor']
                self.__id = cursor['id']

                if is_query:
                    documents = cursor['firstBatch']
                else:
                    documents = cursor['nextBatch']
                self.__data = deque(documents)

                self.__retrieved += len(documents)
            else:
                self.__id = doc['cursor_id']
                self.__data = deque(doc['data'])
                self.__retrieved += doc['number_returned']

        if self.__id == 0:
            self.__killed = True

        if self.__limit and self.__id and self.__limit <= self.__retrieved:
            await self.close()

        return len(self.__data)

    async def __aenter__(self) -> 'Cursor':
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    def __copy__(self) -> 'Cursor':
        """Support function for `copy.copy()`.
        """
        return self._clone(deepcopy=False)

    def __deepcopy__(self, memo) -> 'Cursor':
        """Support function for `copy.deepcopy()`.
        """
        return self._clone(deepcopy=True)

    @property
    def retrieved(self) -> int:
        """The number of documents retrieved so far.
        """
        return self.__retrieved

    def add_option(self, mask: int) -> 'Cursor':
        """Set arbitrary query flags using a bitmask.

        To set the tailable flag:
        cursor.add_option(2)
        """
        if not isinstance(mask, int):
            raise TypeError('mask must be an int')
        self.__check_okay_to_chain()

        self.__query_flags |= mask
        return self

    @property
    def alive(self) -> bool:
        """Does this cursor have the potential to return more data?

        .. note:: Even if :attr:`alive` is True, getting next document can raise
          :exc:`StopAsyncIteration`. :attr:`alive` can also be True while iterating
          a cursor from a failed server. In this case :attr:`alive` will
          return False fail of retrieving the next batch of results from the server.
        """
        return bool(len(self.__data) or (not self.__killed))

    def batch_size(self, batch_size: int) -> 'Cursor':
        """Limits the number of documents returned in one batch. Each batch
        requires a round trip to the server. It can be adjusted to optimize
        performance and limit data transfer.

        .. note:: batch_size can not override MongoDB's internal limits on the
           amount of data it will return to the client in a single batch (i.e
           if you set batch size to 1,000,000,000, MongoDB will currently only
           return 4-16MB of results per batch).

        Raises :exc:`TypeError` if `batch_size` is not an integer.
        Raises :exc:`ValueError` if `batch_size` is less than ``0``.
        Raises :exc:`~pymongo.errors.InvalidOperation` if this
        :class:`Cursor` has already been used. The last `batch_size`
        applied to this cursor takes precedence.

        :Parameters:
          - `batch_size`: The size of each batch of results requested.
        """
        if not isinstance(batch_size, int):
            raise TypeError('batch_size must be an integer')
        if batch_size < 0:
            raise ValueError('batch_size must be >= 0')
        self.__check_okay_to_chain()

        self.__batch_size = batch_size
        return self

    def clone(self) -> 'Cursor':
        """Get a clone of this cursor.

        Returns a new Cursor instance with options matching those that have
        been set on the current instance. The clone will be completely
        unevaluated, even if the current instance has been partially or
        completely evaluated.
        """
        return self._clone(True)

    def comment(self, comment: Union[str, dict]) -> 'Cursor':
        """Adds a 'comment' to the cursor.

        http://docs.mongodb.org/manual/reference/operator/comment/

        :Parameters:
          - `comment`: A string or document

        """
        self.__check_okay_to_chain()
        self.__comment = comment
        return self

    async def count(self, with_limit_and_skip=False) -> int:
        """Get the size of the results set for this query.

        Returns the number of documents in the results set for this query. Does
        not take :meth:`limit` and :meth:`skip` into account by default - set
        `with_limit_and_skip` to ``True`` if that is the desired behavior.
        Raises :class:`~pymongo.errors.OperationFailure` on a database error.

        When used with MongoDB >= 2.6, :meth:`~count` uses any :meth:`~hint`
        applied to the query. In the following example the hint is passed to
        the count command:

          await collection.find({'field': 'value'}).hint('field_1').count()

        The :meth:`count` method obeys the
        :attr:`~aiomongo.collection.Collection.read_preference` of the
        :class:`~aiomongo.collection.Collection` instance on which
        :meth:`~aiomongo.collection.Collection.find` was called.

        :Parameters:
          - `with_limit_and_skip` (optional): take any :meth:`limit` or
            :meth:`skip` that has been applied to this cursor into account when
            getting the count
        """
        validate_boolean('with_limit_and_skip', with_limit_and_skip)

        cmd = SON([('count', self.__collection.name),
                   ('query', self.__spec)])
        if self.__max_time_ms is not None:
            cmd['maxTimeMS'] = self.__max_time_ms
        if self.__comment:
            cmd['$comment'] = self.__comment

        if self.__hint is not None:
            cmd['hint'] = self.__hint

        if with_limit_and_skip:
            if self.__limit:
                cmd['limit'] = self.__limit
            if self.__skip:
                cmd['skip'] = self.__skip

        return await self.__collection._count(cmd)

    async def distinct(self, key: str) -> list:
        """Get a list of distinct values for `key` among all documents
        in the result set of this query.

        Raises :class:`TypeError` if `key` is not an instance of :class:`str`

        The :meth:`distinct` method obeys the
        :attr:`~aiomongo.collection.Collection.read_preference` of the
        :class:`~aiomongo.collection.Collection` instance on which
        :meth:`~aiomongo.collection.Collection.find` was called.

        :Parameters:
          - `key`: name of key for which we want to get the distinct values

        .. seealso:: :meth:`aiomongo.collection.Collection.distinct`
        """
        options = {}
        if self.__spec:
            options['query'] = self.__spec
        if self.__max_time_ms is not None:
            options['maxTimeMS'] = self.__max_time_ms
        if self.__comment:
            options['$comment'] = self.__comment

        return await self.__collection.distinct(key, **options)

    async def explain(self) -> MutableMapping:
        """Returns an explain plan record for this cursor.

        .. mongodoc:: explain
        """

        c = self.clone()
        c.__explain = True

        # always use a hard limit for explains
        if c.__limit:
            c.__limit = -abs(self.__limit)

        async for expl in c:
            return expl

        return None

    def hint(self, index: Union[str, List[tuple]]) -> 'Cursor':
        """Adds a 'hint', telling Mongo the proper index to use for the query.

        Judicious use of hints can greatly improve query
        performance. When doing a query on multiple fields (at least
        one of which is indexed) pass the indexed field as a hint to
        the query. Hinting will not do anything if the corresponding
        index does not exist. Raises
        :class:`~pymongo.errors.InvalidOperation` if this cursor has
        already been used.

        `index` should be an index as passed to
        :meth:`~aiomongo.collection.Collection.create_index`
        (e.g. ``[('field', ASCENDING)]``) or the name of the index.
        If `index` is ``None`` any existing hint for this query is
        cleared. The last hint applied to this cursor takes precedence
        over all others.

        :Parameters:
          - `index`: index to hint on (as an index specifier)
        """
        self.__check_okay_to_chain()
        if index is None:
            self.__hint = None
            return self

        if isinstance(index, str):
            self.__hint = index
        else:
            self.__hint = helpers._index_document(index)
        return self

    def limit(self, limit) -> 'Cursor':
        """Limits the number of results to be returned by this cursor.

        Raises :exc:`TypeError` if `limit` is not an integer. Raises
        :exc:`~pymongo.errors.InvalidOperation` if this :class:`Cursor`
        has already been used. The last `limit` applied to this cursor
        takes precedence. A limit of ``0`` is equivalent to no limit.

        :Parameters:
          - `limit`: the number of results to return

        .. mongodoc:: limit
        """
        if not isinstance(limit, int):
            raise TypeError('limit must be an integer')

        self.__check_okay_to_chain()

        self.__limit = limit
        return self

    def max(self, spec: Union[list, tuple]) -> 'Cursor':
        """Adds `max` operator that specifies upper bound for specific index.

        :Parameters:
          - `spec`: a list of field, limit pairs specifying the exclusive
            upper bound for all keys of a specific index in order.

        """
        if not isinstance(spec, (list, tuple)):
            raise TypeError('spec must be an instance of list or tuple')

        self.__check_okay_to_chain()
        self.__max = SON(spec)
        return self

    def max_scan(self, max_scan: int) -> 'Cursor':
        """Limit the number of documents to scan when performing the query.

        Raises :class:`~pymongo.errors.InvalidOperation` if this
        cursor has already been used. Only the last :meth:`max_scan`
        applied to this cursor has any effect.

        :Parameters:
          - `max_scan`: the maximum number of documents to scan
        """
        self.__check_okay_to_chain()
        self.__max_scan = max_scan
        return self

    def max_time_ms(self, max_time_ms: Optional[int]) -> 'Cursor':
        """Specifies a time limit for a query operation. If the specified
        time is exceeded, the operation will be aborted and
        :exc:`~pymongo.errors.ExecutionTimeout` is raised. If `max_time_ms`
        is ``None`` no limit is applied.

        Raises :exc:`TypeError` if `max_time_ms` is not an integer or ``None``.
        Raises :exc:`~pymongo.errors.InvalidOperation` if this :class:`Cursor`
        has already been used.

        :Parameters:
          - `max_time_ms`: the time limit after which the operation is aborted
        """
        if not isinstance(max_time_ms, int) and max_time_ms is not None:
            raise TypeError('max_time_ms must be an integer or None')
        self.__check_okay_to_chain()

        self.__max_time_ms = max_time_ms
        return self

    def min(self, spec: Union[list, tuple]) -> 'Cursor':
        """Adds `min` operator that specifies lower bound for specific index.

        :Parameters:
          - `spec`: a list of field, limit pairs specifying the inclusive
            lower bound for all keys of a specific index in order.

        .. versionadded:: 2.7
        """
        if not isinstance(spec, (list, tuple)):
            raise TypeError("spec must be an instance of list or tuple")

        self.__check_okay_to_chain()
        self.__min = SON(spec)
        return self

    def rewind(self) -> 'Cursor':
        """Rewind this cursor to its unevaluated state.

        Reset this cursor if it has been partially or completely evaluated.
        Any options that are present on the cursor will remain in effect.
        Future iterating performed on this cursor will cause new queries to
        be sent to the server, even if the resultant data has already been
        retrieved by this cursor.
        """
        self.__data = deque()
        self.__id = None
        self.__retrieved = 0
        self.__killed = False

        return self

    def skip(self, skip: int) -> 'Cursor':
        """Skips the first `skip` results of this cursor.

        Raises :exc:`TypeError` if `skip` is not an integer. Raises
        :exc:`ValueError` if `skip` is less than ``0``. Raises
        :exc:`~pymongo.errors.InvalidOperation` if this :class:`Cursor` has
        already been used. The last `skip` applied to this cursor takes
        precedence.

        :Parameters:
          - `skip`: the number of results to skip
        """
        if not isinstance(skip, int):
            raise TypeError('skip must be an integer')
        if skip < 0:
            raise ValueError('skip must be >= 0')
        self.__check_okay_to_chain()

        self.__skip = skip
        return self

    def sort(self, key_or_list: Union[str, List[tuple]], direction: Optional[int] = None) -> 'Cursor':
        """Sorts this cursor's results.

        Pass a field name and a direction, either
        :data:`~pymongo.ASCENDING` or :data:`~pymongo.DESCENDING`::

            async for doc in collection.find().sort('field', pymongo.ASCENDING):
                print(doc)

        To sort by multiple fields, pass a list of (key, direction) pairs::

            async for doc in collection.find().sort([
                    ('field1', pymongo.ASCENDING),
                    ('field2', pymongo.DESCENDING)]):
                print(doc)

        Beginning with MongoDB version 2.6, text search results can be
        sorted by relevance::

            cursor = db.test.find(
                {'$text': {'$search': 'some words'}},
                {'score': {'$meta': 'textScore'}})

            # Sort by 'score' field.
            cursor.sort([('score', {'$meta': 'textScore'})])

            async for doc in cursor:
                print(doc)

        Raises :class:`~pymongo.errors.InvalidOperation` if this cursor has
        already been used. Only the last :meth:`sort` applied to this
        cursor has any effect.

        :Parameters:
          - `key_or_list`: a single key or a list of (key, direction)
            pairs specifying the keys to sort on
          - `direction` (optional): only used if `key_or_list` is a single
            key, if not given :data:`~pymongo.ASCENDING` is assumed
        """
        self.__check_okay_to_chain()
        keys = helpers._index_list(key_or_list, direction)
        self.__ordering = helpers._index_document(keys)
        return self

    async def to_list(self) -> List[MutableMapping]:
        """ Fetches all data from cursor to in-memory list.
        """
        items = []
        async with self:
            async for item in self:
                items.append(item)
        return items

    def where(self, code: Union[str, Code]) -> 'Cursor':
        """Adds a $where clause to this query.

        The `code` argument must be an instance of :class:`str`
        or :class:`~bson.code.Code` containing a JavaScript expression.
        This expression will be evaluated for each document scanned.
        Only those documents for which the expression evaluates to *true*
        will be returned as results. The keyword *this* refers to the object
        currently being scanned.

        Raises :class:`TypeError` if `code` is not an instance of
        :class:`str`. Raises :class:`~pymongo.errors.InvalidOperation` if this
        :class:`Cursor` has already been used. Only the last call to
        :meth:`where` applied to a :class:`Cursor` has any effect.

        :Parameters:
          - `code`: JavaScript expression to use as a filter
        """
        self.__check_okay_to_chain()
        if not isinstance(code, Code):
            code = Code(code)

        self.__spec['$where'] = code
        return self

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
