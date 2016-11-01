import collections
from typing import Any, Iterable, Optional, Union, List, Tuple, MutableMapping

from bson import ObjectId
from bson.code import Code
from bson.son import SON
from bson.codec_options import CodecOptions
from pymongo import common, helpers, message
from pymongo.collection import ReturnDocument, _NO_OBJ_ERROR
from pymongo.errors import ConfigurationError, InvalidName
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference, _ALL_READ_PREFERENCES
from pymongo.results import DeleteResult, InsertManyResult, InsertOneResult, UpdateResult
from pymongo.write_concern import WriteConcern

import aiomongo
from .bulk import Bulk
from .command_cursor import CommandCursor
from .cursor import Cursor


class Collection:
    def __init__(self, database: 'aiomongo.Database', name: str,
                 read_preference: Optional[Union[_ALL_READ_PREFERENCES]] = None,
                 read_concern: Optional[ReadConcern] = None,
                 codec_options: Optional[CodecOptions] = None, write_concern: Optional[WriteConcern] = None):
        self.database = database
        self.read_preference = read_preference or database.read_preference
        self.read_concern = read_concern or database.read_concern
        self.write_concern = write_concern or database.write_concern
        self.codec_options = codec_options or database.codec_options
        self.name = name

        self.__write_response_codec_options = self.codec_options._replace(
            unicode_decode_error_handler='replace',
            document_class=dict)

    def __str__(self) -> str:
        return '{}.{}'.format(self.database.name, self.name)

    def __repr__(self) -> str:
        return 'Collection({}, {})'.format(self.database.name, self.name)

    async def aggregate(self, pipeline: List[dict], **kwargs) -> CommandCursor:
        """Perform an aggregation using the aggregation framework on this
        collection.

        All optional aggregate parameters should be passed as keyword arguments
        to this method. Valid options include, but are not limited to:

          - `allowDiskUse` (bool): Enables writing to temporary files. When set
            to True, aggregation stages can write data to the _tmp subdirectory
            of the --dbpath directory. The default is False.
          - `maxTimeMS` (int): The maximum amount of time to allow the operation
            to run in milliseconds.
          - `batchSize` (int): The maximum number of documents to return per
            batch. Ignored if the connected mongod or mongos does not support
            returning aggregate results using a cursor, or `useCursor` is
            ``False``.

        The :meth:`aggregate` method obeys the :attr:`read_preference` of this
        :class:`Collection`. Please note that using the ``$out`` pipeline stage
        requires a read preference of
        :attr:`~pymongo.read_preferences.ReadPreference.PRIMARY` (the default).
        The server will raise an error if the ``$out`` pipeline stage is used
        with any other read preference.

        :Parameters:
          - `pipeline`: a list of aggregation pipeline stages
          - `**kwargs` (optional): See list of options above.

        :Returns:
          A :class:`~aiomongo.command_cursor.CommandCursor` over the result
          set.

        .. _aggregate command:
            http://docs.mongodb.org/manual/applications/aggregation
        """

        if not isinstance(pipeline, list):
            raise TypeError('pipeline must be a list')

        if 'explain' in kwargs:
            raise ConfigurationError('The explain option is not supported. '
                                     'Use Database.command instead.')

        cmd = SON([('aggregate', self.name),
                   ('pipeline', pipeline)])

        # Remove things that are not command options.
        batch_size = common.validate_positive_integer_or_none(
            'batchSize', kwargs.pop("batchSize", None))

        if 'cursor' not in kwargs:
            kwargs['cursor'] = {}
        if batch_size is not None:
            kwargs['cursor']['batchSize'] = batch_size

        cmd.update(kwargs)

        connection = self.database.client.get_connection()

        if connection.max_wire_version >= 4 and 'readConcern' not in cmd:
            if pipeline and '$out' in pipeline[-1]:
                result = await connection.command(
                    self.database.name, cmd, self.read_preference, self.codec_options
                )
            else:
                result = await connection.command(
                    self.database.name, cmd, self.read_preference, self.codec_options,
                    read_concern=self.read_concern
                )
        else:
            result = await connection.command(
                self.database.name, cmd, self.read_preference, self.codec_options
            )

        cursor = result['cursor']

        return CommandCursor(connection, self, cursor).batch_size(batch_size or 0)

    async def count(self, filter: Optional[dict] = None, hint: Optional[Union[str, List[Tuple]]] = None,
                    limit: Optional[int] = None, skip: Optional[int] = None, max_time_ms: Optional[int] = None) -> int:
        cmd = SON([('count', self.name)])
        if filter is not None:
            cmd['query'] = filter
        if hint is not None and not isinstance(hint, str):
            cmd['hint'] = helpers._index_document(hint)
        if limit is not None:
            cmd['limit'] = limit
        if skip is not None:
            cmd['skip'] = skip
        if max_time_ms is not None:
            cmd['maxTimeMS'] = max_time_ms

        connection = self.database.client.get_connection()

        result = await connection.command(
            self.database.name, cmd, self.read_preference, self.__write_response_codec_options,
            read_concern=self.read_concern, allowable_errors=['ns missing']
        )

        if result.get('errmsg', '') == 'ns missing':
            return 0

        return int(result["n"])

    async def create_index(self, keys: Union[str, List[Tuple]], **kwargs) -> str:
        """Creates an index on this collection.

        Takes either a single key or a list of (key, direction) pairs.
        The key(s) must be an instance of :class:`basestring`
        (:class:`str` in python 3), and the direction(s) must be one of
        (:data:`~pymongo.ASCENDING`, :data:`~pymongo.DESCENDING`,
        :data:`~pymongo.GEO2D`, :data:`~pymongo.GEOHAYSTACK`,
        :data:`~pymongo.GEOSPHERE`, :data:`~pymongo.HASHED`,
        :data:`~pymongo.TEXT`).

        To create a single key ascending index on the key ``'mike'`` we just
        use a string argument::

          >>> await my_collection.create_index("mike")

        For a compound index on ``'mike'`` descending and ``'eliot'``
        ascending we need to use a list of tuples::

          >>> await my_collection.create_index([("mike", pymongo.DESCENDING),
          ...                             ("eliot", pymongo.ASCENDING)])

        All optional index creation parameters should be passed as
        keyword arguments to this method. For example::

          >>> await my_collection.create_index([("mike", pymongo.DESCENDING)],
          ...                            background=True)

        Valid options include, but are not limited to:

          - `name`: custom name to use for this index - if none is
            given, a name will be generated.
          - `unique`: if ``True`` creates a uniqueness constraint on the index.
          - `background`: if ``True`` this index should be created in the
            background.
          - `sparse`: if ``True``, omit from the index any documents that lack
            the indexed field.
          - `bucketSize`: for use with geoHaystack indexes.
            Number of documents to group together within a certain proximity
            to a given longitude and latitude.
          - `min`: minimum value for keys in a :data:`~pymongo.GEO2D`
            index.
          - `max`: maximum value for keys in a :data:`~pymongo.GEO2D`
            index.
          - `expireAfterSeconds`: <int> Used to create an expiring (TTL)
            collection. MongoDB will automatically delete documents from
            this collection after <int> seconds. The indexed field must
            be a UTC datetime or the data will not expire.
          - `partialFilterExpression`: A document that specifies a filter for
            a partial index.

        See the MongoDB documentation for a full list of supported options by
        server version.

        .. warning:: `dropDups` is not supported by MongoDB 3.0 or newer. The
          option is silently ignored by the server and unique index builds
          using the option will fail if a duplicate value is detected.

        .. note:: `partialFilterExpression` requires server version **>= 3.2**

        :Parameters:
          - `keys`: a single key or a list of (key, direction)
            pairs specifying the index to create
          - `**kwargs` (optional): any additional index creation
            options (see the above list) should be passed as keyword
            arguments

        .. versionchanged:: 3.2
            Added partialFilterExpression to support partial indexes.

        .. mongodoc:: indexes
        """
        keys = helpers._index_list(keys)
        name = kwargs.setdefault('name', helpers._gen_index_name(keys))

        index_doc = helpers._index_document(keys)
        index = {'key': index_doc}
        index.update(kwargs)

        cmd = SON([('createIndexes', self.name), ('indexes', [index])])
        connection = self.database.client.get_connection()

        await connection.command(self.database.name, cmd, ReadPreference.PRIMARY, self.codec_options)
        return name

    async def distinct(self, key: str, filter: Optional[dict] = None, **kwargs) -> dict:
        """Get a list of distinct values for `key` among all documents
        in this collection.

        Raises :class:`TypeError` if `key` is not an instance of :class:`str`

        All optional distinct parameters should be passed as keyword arguments
        to this method. Valid options include:

          - `maxTimeMS` (int): The maximum amount of time to allow the count
            command to run, in milliseconds.

        The :meth:`distinct` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        :Parameters:
          - `key`: name of the field for which we want to get the distinct
            values
          - `filter` (optional): A query document that specifies the documents
            from which to retrieve the distinct values.
          - `**kwargs` (optional): See list of options above.
        """
        if not isinstance(key, str):
            raise TypeError('key must be an instance of str')
        cmd = SON([('distinct', self.name),
                   ('key', key)])
        if filter is not None:
            if 'query' in kwargs:
                raise ConfigurationError('cannott pass both filter and query')
            kwargs['query'] = filter
        cmd.update(kwargs)

        connection = self.database.client.get_connection()

        return (await connection.command(
            self.database.name, cmd, self.read_preference, self.codec_options,
            read_concern=self.read_concern
        ))['values']

    async def drop_index(self, index_or_name):
        """Drops the specified index on this collection.

        Can be used on non-existant collections or collections with no
        indexes.  Raises OperationFailure on an error (e.g. trying to
        drop an index that does not exist). `index_or_name`
        can be either an index name (as returned by `create_index`),
        or an index specifier (as passed to `create_index`). An index
        specifier should be a list of (key, direction) pairs. Raises
        TypeError if index is not an instance of (str, unicode, list).

        .. warning::

          if a custom name was used on index creation (by
          passing the `name` parameter to :meth:`create_index` or
          :meth:`ensure_index`) the index **must** be dropped by name.

        :Parameters:
          - `index_or_name`: index (or name of index) to drop
        """
        name = index_or_name
        if isinstance(index_or_name, list):
            name = helpers._gen_index_name(index_or_name)

        if not isinstance(name, str):
            raise TypeError('index_or_name must be an index name or list')

        cmd = SON([('dropIndexes', self.name), ('index', name)])
        connection = self.database.client.get_connection()
        await connection.command(
            self.database.name, cmd, ReadPreference.PRIMARY, self.codec_options,
            allowable_errors=['ns not found']
        )

    async def drop_indexes(self):
        """Drops all indexes on this collection.

        Can be used on non-existant collections or collections with no indexes.
        Raises OperationFailure on an error.
        """
        await self.drop_index('*')

    def find(self, filter: Optional[dict] = None, projection: Optional[Union[dict, list]] = None,
             skip: int = 0, limit: int = 0, sort: Optional[List[Tuple]] = None, modifiers: Optional[dict] = None,
             batch_size: int = 100, no_cursor_timeout: bool = False) -> Cursor:
        """Query the database.

        The `filter` argument is a prototype document that all results
        must match. For example:

        >>> db.test.find({'hello': 'world'})

        only matches documents that have a key "hello" with value
        "world".  Matches can have other keys *in addition* to
        "hello". The `projection` argument is used to specify a subset
        of fields that should be included in the result documents. By
        limiting results to a certain subset of fields you can cut
        down on network traffic and decoding time.

        Raises :class:`TypeError` if any of the arguments are of
        improper type. Returns an instance of
        :class:`~aiomongo.cursor.Cursor` corresponding to this query.

        The :meth:`find` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        :Parameters:
          - `filter` (optional): a dict object specifying elements which
            must be present for a document to be included in the
            result set
          - `projection` (optional): a list of field names that should be
            returned in the result set or a dict specifying the fields
            to include or exclude. If `projection` is a list '_id' will
            always be returned. Use a dict to exclude fields from
            the result (e.g. projection={'_id': False}).
          - `skip` (optional): the number of documents to omit (from
            the start of the result set) when returning the results
          - `limit` (optional): the maximum number of results to
            return
          - `no_cursor_timeout` (optional): if False (the default), any
            returned cursor is closed by the server after 10 minutes of
            inactivity. If set to True, the returned cursor will never
            time out on the server. Care should be taken to ensure that
            cursors with no_cursor_timeout turned on are properly closed.
          - `sort` (optional): a list of (key, direction) pairs
            specifying the sort order for this query. See
            :meth:`~aiomongo.cursor.Cursor.sort` for details.
          - `modifiers` (optional): A dict specifying the MongoDB `query
            modifiers`_ that should be used for this query. For example::

              >>> db.test.find(modifiers={'$maxTimeMS': 500})

          - `batch_size` (optional): Limits the number of documents returned in
            a single batch.

        .. _query modifiers:
          http://docs.mongodb.org/manual/reference/operator/query-modifier/

        .. mongodoc:: find
        """
        return Cursor(
            self, filter, projection, skip, limit, sort, modifiers, batch_size, no_cursor_timeout
        )

    async def find_one(self, filter: Optional[Union[dict, Any]] = None, projection: Optional[Union[dict, list]] = None,
                       skip: int = 0, sort: Optional[List[Tuple]] = None, max_time_ms: Optional[int] = None,
                       modifiers: Optional[dict] = None) -> Optional[dict]:
        """Get a single document from the database.

        All arguments to :meth:`find` are also valid arguments for
        :meth:`find_one`, although any `limit` argument will be
        ignored. Returns a single document, or ``None`` if no matching
        document is found.

        The :meth:`find_one` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        :Additional to :meth:`find` parameters:

          - `max_time_ms` (optional): a value for max_time_ms may be
            specified as part of `**kwargs`, e.g.

              >>> await find_one(max_time_ms=100)
        """
        if (filter is not None and not
                isinstance(filter, collections.Mapping)):
            filter = {'_id': filter}

        result_cursor = self.find(
            filter=filter, projection=projection, skip=skip, limit=1, sort=sort, modifiers=modifiers
        ).max_time_ms(max_time_ms)

        async for item in result_cursor:
            return item

        return None

    async def group(self, key: Optional[Union[List[str], str, Code]], condition: dict, initial: int, reduce: str,
                    finalize: Optional[str] = None, **kwargs):
        """Perform a query similar to an SQL *group by* operation.

        Returns an array of grouped items.

        The `key` parameter can be:

          - ``None`` to use the entire document as a key.
          - A :class:`list` of keys (each a :class:`str`) to group by.
          - A :class:`str`, or :class:`~bson.code.Code` instance containing a JavaScript
            function to be applied to each document, returning the key
            to group by.

        The :meth:`group` method obeys the :attr:`read_preference` of this
        :class:`Collection`.

        :Parameters:
          - `key`: fields to group by (see above description)
          - `condition`: specification of rows to be
            considered (as a :meth:`find` query specification)
          - `initial`: initial value of the aggregation counter object
          - `reduce`: aggregation function as a JavaScript string
          - `finalize`: function to be called on each object in output list.
          - `**kwargs` (optional): additional arguments to the group command
            may be passed as keyword arguments to this helper method
        """
        group = {}
        if isinstance(key, str):
            group['$keyf'] = Code(key)
        elif key is not None:
            group = {'key': helpers._fields_list_to_dict(key, 'key')}
        group['ns'] = self.name
        group['$reduce'] = Code(reduce)
        group['cond'] = condition
        group['initial'] = initial
        if finalize is not None:
            group['finalize'] = Code(finalize)

        cmd = SON([('group', group)])
        cmd.update(kwargs)

        connection = self.database.client.get_connection()

        return await connection.command(
            self.database.name, cmd, self.read_preference, self.codec_options
        )

    async def insert_one(self, document: MutableMapping, bypass_document_validation: bool = False,
                         check_keys: bool = True) -> InsertOneResult:
        if '_id' not in document:
            document['_id'] = ObjectId()

        write_concern = self.write_concern.document
        acknowledged = write_concern.get('w') != 0

        connection = self.database.client.get_connection()

        if acknowledged:
            command = SON([('insert', self.name),
                           ('ordered', True),
                           ('documents', [document])])

            if bypass_document_validation and connection.max_wire_version >= 4:
                command['bypassDocumentValidation'] = True

            result = await connection.command(
                self.database.name, command, ReadPreference.PRIMARY, self.codec_options
            )

            helpers._check_write_command_response([(0, result)])
        else:
            _, msg, _ = message.insert(
                str(self), [document], check_keys,
                acknowledged, write_concern, False, self.__write_response_codec_options
            )
            await connection.send_message(msg)

        return InsertOneResult(document['_id'], acknowledged)

    async def insert_many(self, documents: Iterable[dict], ordered: bool = True,
                          bypass_document_validation: bool = False) -> InsertManyResult:

        if not isinstance(documents, collections.Iterable) or not documents:
            raise TypeError('documents must be a non-empty list')

        blk = Bulk(self, ordered, bypass_document_validation)
        inserted_ids = []
        for document in documents:
            common.validate_is_document_type('document', document)
            if '_id' not in document:
                document['_id'] = ObjectId()
            blk.ops.append((message._INSERT, document))
            inserted_ids.append(document['_id'])

        write_concern = self.write_concern.document
        acknowledged = write_concern.get('w') != 0

        if acknowledged:
            await blk.execute(write_concern)
        else:
            connection = self.database.client.get_connection()
            _, msg, _ = message.insert(
                str(self), documents, False,
                acknowledged, write_concern, False, self.__write_response_codec_options
            )
            await connection.send_message(msg)

        return InsertManyResult(inserted_ids, self.write_concern.acknowledged)

    async def _update(self, connection: 'aiomongo.Connection', criteria: dict, document: dict,
                      upsert: bool = False, check_keys: bool = True, multi: bool = False,
                      ordered: bool = True, bypass_doc_val: bool = False) -> Optional[dict]:
        """Internal update / replace helper."""
        common.validate_boolean('upsert', upsert)

        concern = self.write_concern.document

        acknowledged = concern.get('w') != 0
        command = SON([('update', self.name),
                       ('ordered', ordered),
                       ('writeConcern', concern),
                       ('updates', [SON([('q', criteria),
                                         ('u', document),
                                         ('multi', multi),
                                         ('upsert', upsert)])])])

        if acknowledged:

            if bypass_doc_val and connection.max_wire_version >= 4:
                command['bypassDocumentValidation'] = True

            result = await connection.command(
                self.database.name, command, ReadPreference.PRIMARY, self.codec_options
            )
            helpers._check_write_command_response([(0, result)])

            # Add the updatedExisting field for compatibility.
            if result.get('n') and 'upserted' not in result:
                result['updatedExisting'] = True
            else:
                result['updatedExisting'] = False
                # MongoDB >= 2.6.0 returns the upsert _id in an array
                # element. Break it out for backward compatibility.
                if 'upserted' in result:
                    result['upserted'] = result['upserted'][0]['_id']
        else:
            _, msg, _ = message.update(
                str(self), upsert, multi, criteria, document, acknowledged, False,
                check_keys, self.__write_response_codec_options
            )
            await connection.send_message(msg)
            result = None

        return result

    async def replace_one(self, filter: dict, replacement: dict, upsert: bool = False,
                          bypass_document_validation: bool = False) -> UpdateResult:
        """Replace a single document matching the filter.

          >>> async for doc in db.test.find({}):
          ...     print(doc)
          ...
          {'x': 1, '_id': ObjectId('54f4c5befba5220aa4d6dee7')}
          >>> result = await db.test.replace_one({'x': 1}, {'y': 1})
          >>> result.matched_count
          1
          >>> result.modified_count
          1
          >>> async for doc in db.test.find({}):
          ...     print(doc)
          ...
          {'y': 1, '_id': ObjectId('54f4c5befba5220aa4d6dee7')}

        The *upsert* option can be used to insert a new document if a matching
        document does not exist.

          >>> result = await db.test.replace_one({'x': 1}, {'x': 1}, True)
          >>> result.matched_count
          0
          >>> result.modified_count
          0
          >>> result.upserted_id
          ObjectId('54f11e5c8891e756a6e1abd4')
          >>> await db.test.find_one({'x': 1})
          {'x': 1, '_id': ObjectId('54f11e5c8891e756a6e1abd4')}

        :Parameters:
          - `filter`: A query that matches the document to replace.
          - `replacement`: The new document.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.
          - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.

        :Returns:
          - An instance of :class:`~pymongo.results.UpdateResult`.

        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**

        .. versionchanged:: 3.2
          Added bypass_document_validation support
        """
        common.validate_is_mapping('filter', filter)
        common.validate_ok_for_replace(replacement)

        connection = self.database.client.get_connection()

        result = await self._update(
            connection, filter, replacement, upsert,
            bypass_doc_val=bypass_document_validation
        )
        return UpdateResult(result, self.write_concern.acknowledged)

    async def update_one(self, filter: dict, update: dict, upsert: bool = False,
                         bypass_document_validation: bool = False) -> UpdateResult:
        """Update a single document matching the filter.

          >>> async for doc in db.test.find():
          ...     print(doc)
          ...
          {'x': 1, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}
          >>> result = await db.test.update_one({'x': 1}, {'$inc': {'x': 3}})
          >>> result.matched_count
          1
          >>> result.modified_count
          1
          >>> async for doc in db.test.find():
          ...     print(doc)
          ...
          {'x': 4, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}

        :Parameters:
          - `filter`: A query that matches the document to update.
          - `update`: The modifications to apply.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.
          - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.

        :Returns:
          - An instance of :class:`~pymongo.results.UpdateResult`.

        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**

        .. versionchanged:: 3.2
          Added bypass_document_validation support
        """

        common.validate_is_mapping('filter', filter)
        common.validate_ok_for_update(update)

        connection = self.database.client.get_connection()
        result = await self._update(
            connection, filter, update, upsert, check_keys=False,
            bypass_doc_val=bypass_document_validation
        )
        return UpdateResult(result, self.write_concern.acknowledged)

    async def update_many(self, filter: dict, update: dict, upsert: bool = False,
                          bypass_document_validation: bool = False) -> UpdateResult:
        """Update one or more documents that match the filter.

          >>> async for doc in db.test.find():
          ...     print(doc)
          ...
          {'x': 1, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}
          >>> result = await db.test.update_many({'x': 1}, {'$inc': {'x': 3}})
          >>> result.matched_count
          3
          >>> result.modified_count
          3
          >>> async for doc in db.test.find():
          ...     print(doc)
          ...
          {'x': 4, '_id': 0}
          {'x': 4, '_id': 1}
          {'x': 4, '_id': 2}

        :Parameters:
          - `filter`: A query that matches the documents to update.
          - `update`: The modifications to apply.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.
          - `bypass_document_validation`: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.

        :Returns:
          - An instance of :class:`~pymongo.results.UpdateResult`.

        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**

        .. versionchanged:: 3.2
          Added bypass_document_validation support
        """
        common.validate_is_mapping('filter', filter)
        common.validate_ok_for_update(update)

        connection = self.database.client.get_connection()
        result = await self._update(
            connection, filter, update, upsert,
            check_keys=False, multi=True,
            bypass_doc_val=bypass_document_validation
        )
        return UpdateResult(result, self.write_concern.acknowledged)

    async def _delete(self, connection: 'aiomongo.Connection', criteria: dict, multi: bool,
                      ordered: bool = True) -> Optional[dict]:
        """Internal delete helper."""

        common.validate_is_mapping('filter', criteria)
        concern = self.write_concern.document
        acknowledged = concern.get('w') != 0
        command = SON([('delete', self.name),
                       ('ordered', ordered),
                       ('writeConcern', concern),
                       ('deletes', [SON([('q', criteria),
                                         ('limit', int(not multi))])])])

        if acknowledged:
            # Delete command
            result = await connection.command(
                self.database.name, command, ReadPreference.PRIMARY,
                self.__write_response_codec_options
            )
            helpers._check_write_command_response([(0, result)])
            return result

        _, msg, _ = message.delete(
            str(self), criteria, False, concern, self.__write_response_codec_options,
            int(not multi)
        )
        await connection.send_message(msg)

        return None

    async def delete_one(self, filter: dict) -> DeleteResult:
        """Delete a single document matching the filter.

          >>> await db.test.count({'x': 1})
          3
          >>> result = await db.test.delete_one({'x': 1})
          >>> result.deleted_count
          1
          >>> await db.test.count({'x': 1})
          2

        :Parameters:
          - `filter`: A query that matches the document to delete.
        :Returns:
          - An instance of :class:`~pymongo.results.DeleteResult`.
        """

        connection = self.database.client.get_connection()
        result = await self._delete(connection, filter, False)
        return DeleteResult(result, self.write_concern.acknowledged)

    async def delete_many(self, filter: dict) -> DeleteResult:
        """Delete one or more documents matching the filter.

          >>> await db.test.count({'x': 1})
          3
          >>> result = await db.test.delete_many({'x': 1})
          >>> result.deleted_count
          3
          >>> await db.test.count({'x': 1})
          0

        :Parameters:
          - `filter`: A query that matches the documents to delete.
        :Returns:
          - An instance of :class:`~pymongo.results.DeleteResult`.
        """
        connection = self.database.client.get_connection()
        result = await self._delete(connection, filter, True)
        return DeleteResult(result, self.write_concern.acknowledged)

    async def reindex(self) -> None:
        """Rebuilds all indexes on this collection.

        .. warning:: reindex blocks all other operations (indexes
           are built in the foreground) and will be slow for large
           collections.
        """
        cmd = SON([('reIndex', self.name)])

        connection = self.database.client.get_connection()

        await connection.command(self.database.name, cmd, ReadPreference.PRIMARY, self.codec_options)

    async def map_reduce(self, map: Code, reduce: Code, out: str,
                         full_response: bool = False, **kwargs) -> Union[dict, 'Collection']:
        """Perform a map/reduce operation on this collection.

        If `full_response` is ``False`` (default) returns a
        :class:`~pymongo.collection.Collection` instance containing
        the results of the operation. Otherwise, returns the full
        response from the server to the `map reduce command`_.

        :Parameters:
          - `map`: map function (as a JavaScript string)
          - `reduce`: reduce function (as a JavaScript string)
          - `out`: output collection name or `out object` (dict). See
            the `map reduce command`_ documentation for available options.
            Note: `out` options are order sensitive. :class:`~bson.son.SON`
            can be used to specify multiple options.
            e.g. SON([('replace', <collection name>), ('db', <database name>)])
          - `full_response` (optional): if ``True``, return full response to
            this command - otherwise just return the result collection
          - `**kwargs` (optional): additional arguments to the
            `map reduce command`_ may be passed as keyword arguments to this
            helper method, e.g.::

            >>> await db.test.map_reduce(map, reduce, 'myresults', limit=2)

        .. note:: The :meth:`map_reduce` method does **not** obey the
           :attr:`read_preference` of this :class:`Collection`. To run
           mapReduce on a secondary use the :meth:`inline_map_reduce` method
           instead.

        .. _map reduce command: http://docs.mongodb.org/manual/reference/command/mapReduce/

        .. mongodoc:: mapreduce
        """
        if not isinstance(out, (str, collections.Mapping)):
            raise TypeError('"out" must be an instance of str or a mapping')

        cmd = SON([('mapreduce', self.name),
                   ('map', map),
                   ('reduce', reduce),
                   ('out', out)])
        cmd.update(kwargs)

        connection = self.database.client.get_connection()

        if connection.max_wire_version >= 4 and 'readConcern' not in cmd and 'inline' in cmd['out']:
            response = await connection.command(
                self.database.name, cmd, ReadPreference.PRIMARY, self.codec_options,
                read_concern=self.read_concern)
        else:
            response = await connection.command(
                self.database.name, cmd, ReadPreference.PRIMARY, self.codec_options)

        if full_response or not response.get('result'):
            return response
        elif isinstance(response['result'], dict):
            dbase = response['result']['db']
            coll = response['result']['collection']
            return self.database.client[dbase][coll]
        else:
            return self.database[response['result']]

    async def inline_map_reduce(self, map: Code, reduce: Code, full_response: bool = False, **kwargs) -> dict:
        """Perform an inline map/reduce operation on this collection.

        Perform the map/reduce operation on the server in RAM. A result
        collection is not created. The result set is returned as a list
        of documents.

        If `full_response` is ``False`` (default) returns the
        result documents in a list. Otherwise, returns the full
        response from the server to the `map reduce command`_.

        The :meth:`inline_map_reduce` method obeys the :attr:`read_preference`
        of this :class:`Collection`.

        :Parameters:
          - `map`: map function (as a JavaScript string)
          - `reduce`: reduce function (as a JavaScript string)
          - `full_response` (optional): if ``True``, return full response to
            this command - otherwise just return the result collection
          - `**kwargs` (optional): additional arguments to the
            `map reduce command`_ may be passed as keyword arguments to this
            helper method, e.g.::

            >>> await db.test.inline_map_reduce(map, reduce, limit=2)
        """
        cmd = SON([('mapreduce', self.name),
                   ('map', map),
                   ('reduce', reduce),
                   ('out', {'inline': 1})])
        cmd.update(kwargs)

        connection = self.database.client.get_connection()

        if connection.max_wire_version >= 4 and 'readConcern' not in cmd:
            res = await connection.command(
                self.database.name, cmd, self.read_preference, self.codec_options,
                read_concern=self.read_concern
            )
        else:
            res = await connection.command(
                self.database.name, cmd, self.read_preference, self.codec_options,
            )

        if full_response:
            return res
        else:
            return res.get('results')

    async def list_indexes(self) -> CommandCursor:
        """Get a cursor over the index documents for this collection.

          >>> async with await db.test.list_indexes() as cursor:
          ...     async for index in cursor:
          ...         print(index)
          ...
          SON([('v', 1), ('key', SON([('_id', 1)])),
               ('name', '_id_'), ('ns', 'test.test')])

        :Returns:
          An instance of :class:`~aiomongo.command_cursor.CommandCursor`.

        """
        codec_options = CodecOptions(SON)
        coll = self.with_options(codec_options)

        cmd = SON([('listIndexes', self.name), ('cursor', {})])

        connection = self.database.client.get_connection()

        cursor = (await connection.command(
            self.database.name, cmd, ReadPreference.PRIMARY, codec_options
        ))['cursor']

        return CommandCursor(connection, coll, cursor)

    async def rename(self, new_name: str, **kwargs) -> None:
        """Rename this collection.

        If operating in auth mode, client must be authorized as an
        admin to perform this operation. Raises :class:`TypeError` if
        `new_name` is not an instance of :class:`basestring`
        (:class:`str` in python 3). Raises :class:`~pymongo.errors.InvalidName`
        if `new_name` is not a valid collection name.

        :Parameters:
          - `new_name`: new name for this collection
          - `**kwargs` (optional): additional arguments to the rename command
            may be passed as keyword arguments to this helper method
            (i.e. ``dropTarget=True``)
        """
        if not isinstance(new_name, str):
            raise TypeError('new_name must be an instance of str')

        if not new_name or '..' in new_name:
            raise InvalidName('collection names cannot be empty')
        if new_name[0] == '.' or new_name[-1] == '.':
            raise InvalidName('collection names must not start or end with \'.\'')
        if '$' in new_name and not new_name.startswith('oplog.$main'):
            raise InvalidName('collection names must not contain \'$\'')

        new_name = '{}.{}'.format(self.database.name, new_name)
        cmd = SON([('renameCollection', str(self)), ('to', new_name)])
        cmd.update(kwargs)

        connection = self.database.client.get_connection()
        await connection.command('admin', cmd, ReadPreference.PRIMARY, self.codec_options)

    async def index_information(self) -> dict:
        """Get information on this collection's indexes.

        Returns a dictionary where the keys are index names (as
        returned by create_index()) and the values are dictionaries
        containing information about each index. The dictionary is
        guaranteed to contain at least a single key, ``"key"`` which
        is a list of (key, direction) pairs specifying the index (as
        passed to create_index()). It will also contain any other
        metadata about the indexes, except for the ``"ns"`` and
        ``"name"`` keys, which are cleaned. Example output might look
        like this:

        >>> await db.test.ensure_index("x", unique=True)
        'x_1'
        >>> await db.test.index_information()
        {'_id_': {'key': [('_id', 1)]},
         'x_1': {'unique': True, 'key': [('x', 1)]}}
        """
        info = {}
        async with (await self.list_indexes()) as cursor:
            async for index in cursor:
                index['key'] = index['key'].items()
                index = dict(index)
                info[index.pop('name')] = index
        return info

    def with_options(self, codec_options: Optional[CodecOptions] = None,
                     read_preference: Optional[Union[_ALL_READ_PREFERENCES]] = None,
                     write_concern: Optional[WriteConcern] = None,
                     read_concern: Optional[ReadConcern] = None) -> 'Collection':
        """Get a clone of this collection changing the specified settings.

          >>> coll1.read_preference
          Primary()
          >>> from pymongo import ReadPreference
          >>> coll2 = coll1.with_options(read_preference=ReadPreference.SECONDARY)
          >>> coll1.read_preference
          Primary()
          >>> coll2.read_preference
          Secondary(tag_sets=None)

        :Parameters:
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) the :attr:`codec_options` of this :class:`Collection`
            is used.
          - `read_preference` (optional): The read preference to use. If
            ``None`` (the default) the :attr:`read_preference` of this
            :class:`Collection` is used. See :mod:`~pymongo.read_preferences`
            for options.
          - `write_concern` (optional): An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) the :attr:`write_concern` of this :class:`Collection`
            is used.
          - `read_concern` (optional): An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) the :attr:`read_concern` of this :class:`Collection`
            is used.
        """
        return Collection(self.database,
                          self.name,
                          read_preference or self.read_preference,
                          read_concern or self.read_concern,
                          codec_options or self.codec_options,
                          write_concern or self.write_concern)

    async def __find_and_modify(self, filter: dict, projection: Optional[Union[list, dict]],
                                sort: Optional[List[tuple]], upsert: Optional[bool] = None,
                                return_document: bool = ReturnDocument.BEFORE, **kwargs) -> dict:
        """Internal findAndModify helper."""
        common.validate_is_mapping('filter', filter)
        if not isinstance(return_document, bool):
            raise ValueError('return_document must be ReturnDocument.BEFORE or ReturnDocument.AFTER')
        cmd = SON([('findAndModify', self.name),
                   ('query', filter),
                   ('new', return_document)])
        cmd.update(kwargs)
        if projection is not None:
            cmd['fields'] = helpers._fields_list_to_dict(projection, 'projection')
        if sort is not None:
            cmd['sort'] = helpers._index_document(sort)
        if upsert is not None:
            common.validate_boolean('upsert', upsert)
            cmd['upsert'] = upsert

        connection = self.database.client.get_connection()
        if connection.max_wire_version >= 4 and 'writeConcern' not in cmd:
            wc_doc = self.write_concern.document
            if wc_doc:
                cmd['writeConcern'] = wc_doc

        out = await connection.command(
            self.database.name, cmd, ReadPreference.PRIMARY, self.codec_options,
            allowable_errors=[_NO_OBJ_ERROR]
        )
        helpers._check_write_command_response([(0, out)])
        return out.get('value')

    async def find_one_and_delete(self, filter: dict, projection: Optional[Union[list, dict]] = None,
                                  sort: Optional[List[tuple]] = None, **kwargs) -> dict:
        """Finds a single document and deletes it, returning the document.

          >>> await db.test.count({'x': 1})
          2
          >>> await db.test.find_one_and_delete({'x': 1})
          {'x': 1, '_id': ObjectId('54f4e12bfba5220aa4d6dee8')}
          >>> await db.test.count({'x': 1})
          1

        If multiple documents match *filter*, a *sort* can be applied.

          >>> async for doc in db.test.find({'x': 1}):
          ...     print(doc)
          ...
          {'x': 1, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}
          >>> await db.test.find_one_and_delete(
          ...     {'x': 1}, sort=[('_id', pymongo.DESCENDING)])
          {'x': 1, '_id': 2}

        The *projection* option can be used to limit the fields returned.

          >>> await db.test.find_one_and_delete({'x': 1}, projection={'_id': False})
          {'x': 1}

        :Parameters:
          - `filter`: A query that matches the document to delete.
          - `projection` (optional): a list of field names that should be
            returned in the result document or a mapping specifying the fields
            to include or exclude. If `projection` is a list "_id" will
            always be returned. Use a mapping to exclude fields from
            the result (e.g. projection={'_id': False}).
          - `sort` (optional): a list of (key, direction) pairs
            specifying the sort order for the query. If multiple documents
            match the query, they are sorted and the first is deleted.
          - `**kwargs` (optional): additional command arguments can be passed
            as keyword arguments (for example maxTimeMS can be used with
            recent server versions).

        .. versionchanged:: 3.2
           Respects write concern.

        .. warning:: Starting in PyMongo 3.2, this command uses the
           :class:`~pymongo.write_concern.WriteConcern` of this
           :class:`~pymongo.collection.Collection` when connected to MongoDB >=
           3.2. Note that using an elevated write concern with this command may
           be slower compared to using the default write concern.

        """
        kwargs['remove'] = True
        return await self.__find_and_modify(filter, projection, sort, **kwargs)

    async def find_one_and_replace(self, filter: dict, replacement: dict,
                                   projection: Optional[Union[list, dict]] = None,
                                   sort: Optional[List[tuple]] = None, upsert: bool = False,
                                   return_document: bool = ReturnDocument.BEFORE, **kwargs) -> dict:
        """Finds a single document and replaces it, returning either the
        original or the replaced document.

        The :meth:`find_one_and_replace` method differs from
        :meth:`find_one_and_update` by replacing the document matched by
        *filter*, rather than modifying the existing document.

          >>> async for doc in db.test.find({}):
          ...     print(doc)
          ...
          {'x': 1, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}
          >>> await db.test.find_one_and_replace({'x': 1}, {'y': 1})
          {'x': 1, '_id': 0}
          >>> async for doc in db.test.find({}):
          ...     print(doc)
          ...
          {'y': 1, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}

        :Parameters:
          - `filter`: A query that matches the document to replace.
          - `replacement`: The replacement document.
          - `projection` (optional): A list of field names that should be
            returned in the result document or a mapping specifying the fields
            to include or exclude. If `projection` is a list "_id" will
            always be returned. Use a mapping to exclude fields from
            the result (e.g. projection={'_id': False}).
          - `sort` (optional): a list of (key, direction) pairs
            specifying the sort order for the query. If multiple documents
            match the query, they are sorted and the first is replaced.
          - `upsert` (optional): When ``True``, inserts a new document if no
            document matches the query. Defaults to ``False``.
          - `return_document`: If
            :attr:`ReturnDocument.BEFORE` (the default),
            returns the original document before it was replaced, or ``None``
            if no document matches. If
            :attr:`ReturnDocument.AFTER`, returns the replaced
            or inserted document.
          - `**kwargs` (optional): additional command arguments can be passed
            as keyword arguments (for example maxTimeMS can be used with
            recent server versions).

        .. versionchanged:: 3.2
           Respects write concern.

        .. warning:: Starting in PyMongo 3.2, this command uses the
           :class:`~pymongo.write_concern.WriteConcern` of this
           :class:`~pymongo.collection.Collection` when connected to MongoDB >=
           3.2. Note that using an elevated write concern with this command may
           be slower compared to using the default write concern.

        """
        common.validate_ok_for_replace(replacement)
        kwargs['update'] = replacement
        return await self.__find_and_modify(filter, projection,
                                            sort, upsert, return_document, **kwargs)

    async def find_one_and_update(self, filter: dict, update: dict,
                                  projection: Optional[Union[list, dict]] = None,
                                  sort: Optional[List[tuple]] = None, upsert: bool = False,
                                  return_document: bool = ReturnDocument.BEFORE, **kwargs) -> dict:
        """Finds a single document and updates it, returning either the
        original or the updated document.

          >>> await db.test.find_one_and_update(
          ...    {'_id': 665}, {'$inc': {'count': 1}, '$set': {'done': True}})
          {'_id': 665, 'done': False, 'count': 25}}

        By default :meth:`find_one_and_update` returns the original version of
        the document before the update was applied. To return the updated
        version of the document instead, use the *return_document* option.

          >>> from pymongo import ReturnDocument
          >>> await db.example.find_one_and_update(
          ...     {'_id': 'userid'},
          ...     {'$inc': {'seq': 1}},
          ...     return_document=ReturnDocument.AFTER)
          {'_id': 'userid', 'seq': 1}

        You can limit the fields returned with the *projection* option.

          >>> await db.example.find_one_and_update(
          ...     {'_id': 'userid'},
          ...     {'$inc': {'seq': 1}},
          ...     projection={'seq': True, '_id': False},
          ...     return_document=ReturnDocument.AFTER)
          {'seq': 2}

        The *upsert* option can be used to create the document if it doesn't
        already exist.

          >>> await db.example.delete_many({}).deleted_count
          1
          >>> await db.example.find_one_and_update(
          ...     {'_id': 'userid'},
          ...     {'$inc': {'seq': 1}},
          ...     projection={'seq': True, '_id': False},
          ...     upsert=True,
          ...     return_document=ReturnDocument.AFTER)
          {'seq': 1}

        If multiple documents match *filter*, a *sort* can be applied.

          >>> async for doc in db.test.find({'done': True}):
          ...     print(doc)
          ...
          {'_id': 665, 'done': True, 'result': {'count': 26}}
          {'_id': 701, 'done': True, 'result': {'count': 17}}
          >>> await db.test.find_one_and_update(
          ...     {'done': True},
          ...     {'$set': {'final': True}},
          ...     sort=[('_id', pymongo.DESCENDING)])
          {'_id': 701, 'done': True, 'result': {'count': 17}}

        :Parameters:
          - `filter`: A query that matches the document to update.
          - `update`: The update operations to apply.
          - `projection` (optional): A list of field names that should be
            returned in the result document or a mapping specifying the fields
            to include or exclude. If `projection` is a list "_id" will
            always be returned. Use a dict to exclude fields from
            the result (e.g. projection={'_id': False}).
          - `sort` (optional): a list of (key, direction) pairs
            specifying the sort order for the query. If multiple documents
            match the query, they are sorted and the first is updated.
          - `upsert` (optional): When ``True``, inserts a new document if no
            document matches the query. Defaults to ``False``.
          - `return_document`: If
            :attr:`ReturnDocument.BEFORE` (the default),
            returns the original document before it was updated, or ``None``
            if no document matches. If
            :attr:`ReturnDocument.AFTER`, returns the updated
            or inserted document.
          - `**kwargs` (optional): additional command arguments can be passed
            as keyword arguments (for example maxTimeMS can be used with
            recent server versions).

        .. versionchanged:: 3.2
           Respects write concern.

        .. warning:: Starting in PyMongo 3.2, this command uses the
           :class:`~pymongo.write_concern.WriteConcern` of this
           :class:`~pymongo.collection.Collection` when connected to MongoDB >=
           3.2. Note that using an elevated write concern with this command may
           be slower compared to using the default write concern.

        """
        common.validate_ok_for_update(update)
        kwargs['update'] = update
        return await self.__find_and_modify(filter, projection,
                                            sort, upsert, return_document, **kwargs)

    def __iter__(self) -> 'Collection':
        return self

    def __next__(self):
        raise TypeError('"Collection" object is not iterable')

    def __call__(self, *args, **kwargs):
        """This is only here so that some API misusages are easier to debug.
        """
        if '.' not in self.name:
            raise TypeError('"Collection" object is not callable. If you '
                            'meant to call the "{}" method on a "Database" '
                            'object it is failing because no such method '
                            "exists.".format(self.name))
        raise TypeError('"Collection" object is not callable. If you meant to '
                        'call the "{}" method on a "Collection" object it is '
                        'failing because no such method exists.'.format(self.name.split(".")[-1]))
