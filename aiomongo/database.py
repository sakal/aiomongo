import warnings
from typing import Any, List, MutableMapping, Optional, Union

from bson.codec_options import CodecOptions, DEFAULT_CODEC_OPTIONS
from bson.dbref import DBRef
from bson.son import SON
from pymongo import auth, common
from pymongo.errors import CollectionInvalid, ConfigurationError, OperationFailure
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import _ALL_READ_PREFERENCES, ReadPreference
from pymongo.write_concern import WriteConcern

import aiomongo
from .collection import Collection
from .command_cursor import CommandCursor


class Database:
    """Get a database by client and name.

    Raises :class:`TypeError` if `name` is not an instance of
    :class:`str`. Raises
    :class:`~pymongo.errors.InvalidName` if `name` is not a valid
    database name.

    :Parameters:
      - `client`: A :class:`~aiomongo.client.AioMongoClient` instance.
      - `name`: The database name.
      - `codec_options` (optional): An instance of
        :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
        default) client.codec_options is used.
      - `read_preference` (optional): The read preference to use. If
        ``None`` (the default) client.read_preference is used.
      - `write_concern` (optional): An instance of
        :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
        default) client.write_concern is used.
      - `read_concern` (optional): An instance of
        :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
        default) client.read_concern is used.

    .. mongodoc:: databases

    .. versionchanged:: 3.2
       Added the read_concern option.

    """
    def __init__(self, client: 'aiomongo.AioMongoClient', name: str,
                 read_preference: Optional[Union[_ALL_READ_PREFERENCES]] = None,
                 read_concern: Optional[ReadConcern] = None, codec_options: Optional[CodecOptions] = None,
                 write_concern: Optional[WriteConcern] = None):
        self.client = client
        self.name = name
        self.options = client.options
        self.read_preference = read_preference or self.options.read_preference
        self.read_concern = read_concern or self.options.read_concern
        self.codec_options = codec_options or self.options.codec_options
        self.write_concern = write_concern or self.options.write_concern

    def __getitem__(self, collection_name: str) -> Collection:
        return Collection(self, collection_name)

    def __getattr__(self, collection_name: str) -> Collection:
        if collection_name.startswith('_'):
            raise AttributeError(
                'Database has no attribute {}. To access the {} collection, use database[{}].'.format(
                    collection_name, collection_name, collection_name)
            )
        return self.__getitem__(collection_name)

    def __repr__(self) -> str:
        return 'Database({})'.format(self.name)

    def __str__(self) -> str:
        return self.name

    def __eq__(self, other: 'Database') -> bool:
        if isinstance(other, Database):
            return (self.client == other.client and
                    self.name == other.name)
        return NotImplemented

    async def _command(self, connection: 'aiomongo.Connection', command: Union[str, dict], value: Any=1,
                       check: bool = True, allowable_errors: Optional[List[str]] = None,
                       read_preference: Union[_ALL_READ_PREFERENCES] = ReadPreference.PRIMARY,
                       codec_options: CodecOptions = DEFAULT_CODEC_OPTIONS, **kwargs) -> MutableMapping:
        """Internal command helper."""
        if isinstance(command, str):
            command = SON([(command, value)])
        command.update(kwargs)

        return await connection.command(
            self.name, command, read_preference, codec_options, check=check,
            allowable_errors=allowable_errors
        )

    async def command(self, command: Union[str, dict], value: Any = 1, check: bool = True,
                      allowable_errors: Optional[List[str]] = None,
                      read_preference: Union[_ALL_READ_PREFERENCES] = ReadPreference.PRIMARY,
                      codec_options: CodecOptions = DEFAULT_CODEC_OPTIONS, **kwargs) -> dict:
        """Issue a MongoDB command.

        Send command `command` to the database and return the
        response. If `command` is an instance of :class:`str`
        then the command {`command`: `value`}
        will be sent. Otherwise, `command` must be an instance of
        :class:`dict` and will be sent as is.

        Any additional keyword arguments will be added to the final
        command document before it is sent.

        For example, a command like ``{buildinfo: 1}`` can be sent
        using:

        >>> await db.command('buildinfo')

        For a command where the value matters, like ``{collstats:
        collection_name}`` we can do:

        >>> await db.command('collstats', collection_name)

        For commands that take additional arguments we can use
        kwargs. So ``{filemd5: object_id, root: file_root}`` becomes:

        >>> await db.command('filemd5', object_id, root=file_root)

        :Parameters:
          - `command`: document representing the command to be issued,
            or the name of the command (for simple commands only).

            .. note:: the order of keys in the `command` document is
               significant (the "verb" must come first), so commands
               which require multiple keys (e.g. `findandmodify`)
               should use an instance of :class:`~bson.son.SON` or
               a string and kwargs instead of a Python `dict`.

          - `value` (optional): value to use for the command verb when
            `command` is passed as a string
          - `check` (optional): check the response for errors, raising
            :class:`~pymongo.errors.OperationFailure` if there are any
          - `allowable_errors`: if `check` is ``True``, error messages
            in this list will be ignored by error-checking
          - `read_preference`: The read preference for this operation.
            See :mod:`~pymongo.read_preferences` for options.
          - `codec_options`: A :class:`~bson.codec_options.CodecOptions`
            instance.
          - `**kwargs` (optional): additional keyword arguments will
            be added to the command document before it is sent

        .. note:: :meth:`command` does **not** obey :attr:`read_preference`
           or :attr:`codec_options`. You must use the `read_preference` and
           `codec_options` parameters instead.

        """

        connection = await self.client.get_connection()

        return await self._command(
            connection, command, value, check, allowable_errors, read_preference,
            codec_options, **kwargs
        )

    async def _list_collections(self, connection: 'aiomongo.Connection',
                                criteria: Optional[dict] = None) -> CommandCursor:

        criteria = criteria or {}
        cmd = SON([('listCollections', 1), ('cursor', {})])
        if criteria:
            cmd['filter'] = criteria

        coll = self['$cmd']
        cursor = (await self._command(connection, cmd))['cursor']

        return CommandCursor(connection, coll, cursor)

    async def collection_names(self, include_system_collections: bool = True) -> List[str]:
        """Get a list of all the collection names in this database.

        :Parameters:
          - `include_system_collections` (optional): if ``False`` list
            will not include system collections (e.g ``system.indexes``)
        """
        connection = await self.client.get_connection()

        results_cursor = await self._list_collections(connection)

        names = []

        async with results_cursor as cursor:
            async for result in cursor:
                names.append(result['name'])

        if not include_system_collections:
            names = [name for name in names if not name.startswith('system.')]

        return names

    async def create_collection(self, name, codec_options=None,
                                read_preference=None, write_concern=None,
                                read_concern=None, **kwargs):
        """Create a new :class:`~aiomongo.collection.Collection` in this
        database.

        Normally collection creation is automatic. This method should
        only be used to specify options on
        creation. :class:`~pymongo.errors.CollectionInvalid` will be
        raised if the collection already exists.

        Options should be passed as keyword arguments to this method. Supported
        options vary with MongoDB release. Some examples include:

          - "size": desired initial size for the collection (in
            bytes). For capped collections this size is the max
            size of the collection.
          - "capped": if True, this is a capped collection
          - "max": maximum number of objects if capped (optional)

        See the MongoDB documentation for a full list of supported options by
        server version.

        :Parameters:
          - `name`: the name of the collection to create
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) the :attr:`codec_options` of this :class:`Database` is
            used.
          - `read_preference` (optional): The read preference to use. If
            ``None`` (the default) the :attr:`read_preference` of this
            :class:`Database` is used.
          - `write_concern` (optional): An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) the :attr:`write_concern` of this :class:`Database` is
            used.
          - `read_concern` (optional): An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) the :attr:`read_concern` of this :class:`Database` is
            used.
          - `**kwargs` (optional): additional keyword arguments will
            be passed as options for the create collection command

        """
        if name in await self.collection_names():
            raise CollectionInvalid('collection {} already exists'.format(name))

        coll = Collection(self, name, read_preference, read_concern,
                          codec_options, write_concern)
        await coll._create(kwargs)

        return coll

    async def drop_collection(self, name_or_collection: Union[str, Collection]) -> None:
        """Drop a collection.

        :Parameters:
          - `name_or_collection`: the name of a collection to drop or the
            collection object itself
        """
        name = name_or_collection
        if isinstance(name, Collection):
            name = name.name

        if not isinstance(name, str):
            raise TypeError('name_or_collection must be an instance of str or Collection')

        await self.command('drop', value=name, allowable_errors=['ns not found'])

    def get_collection(self, name: str, codec_options: Optional[CodecOptions] = None,
                       read_preference: Optional[Union[_ALL_READ_PREFERENCES]] = None,
                       write_concern: Optional[WriteConcern] = None,
                       read_concern: Optional[ReadConcern] = None):
        """Get a :class:`~aiomongo.collection.Collection` with the given name
        and options.

        Useful for creating a :class:`~aiomongo.collection.Collection` with
        different codec options, read preference, and/or write concern from
        this :class:`Database`.

          >>> db.read_preference
          Primary()
          >>> coll1 = db.test
          >>> coll1.read_preference
          Primary()
          >>> from pymongo import ReadPreference
          >>> coll2 = db.get_collection(
          ...     'test', read_preference=ReadPreference.SECONDARY)
          >>> coll2.read_preference
          Secondary(tag_sets=None)

        :Parameters:
          - `name`: The name of the collection - a string.
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) the :attr:`codec_options` of this :class:`Database` is
            used.
          - `read_preference` (optional): The read preference to use. If
            ``None`` (the default) the :attr:`read_preference` of this
            :class:`Database` is used. See :mod:`~pymongo.read_preferences`
            for options.
          - `write_concern` (optional): An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) the :attr:`write_concern` of this :class:`Database` is
            used.
          - `read_concern` (optional): An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) the :attr:`read_concern` of this :class:`Database` is
            used.
        """
        return Collection(
            self, name, read_preference, read_concern,
            codec_options, write_concern)

    async def validate_collection(self, name_or_collection: Union[str, Collection],
                                  scandata: bool = False, full: bool = False) -> dict:
        """Validate a collection.

        Returns a dict of validation info. Raises CollectionInvalid if
        validation fails.

        :Parameters:
          - `name_or_collection`: A Collection object or the name of a
            collection to validate.
          - `scandata`: Do extra checks beyond checking the overall
            structure of the collection.
          - `full`: Have the server do a more thorough scan of the
            collection. Use with `scandata` for a thorough scan
            of the structure of the collection and the individual
            documents.
        """
        name = name_or_collection
        if isinstance(name, Collection):
            name = name.name

        if not isinstance(name, str):
            raise TypeError('name_or_collection must be an instance of str or Collection')

        result = await self.command('validate', value=name, scandata=scandata, full=full)

        valid = True
        # Pre 1.9 results
        if 'result' in result:
            info = result['result']
            if info.find('exception') != -1 or info.find('corrupt') != -1:
                raise CollectionInvalid('{} invalid: {}'.format(name, info))
        # Sharded results
        elif 'raw' in result:
            for res in result['raw'].values():
                if 'result' in res:
                    info = res['result']
                    if (info.find('exception') != -1 or
                                info.find('corrupt') != -1):
                        raise CollectionInvalid('{} invalid: '
                                                '{}'.format(name, info))
                elif not res.get('valid', False):
                    valid = False
                    break
        # Post 1.9 non-sharded results.
        elif not result.get('valid', False):
            valid = False

        if not valid:
            raise CollectionInvalid('{} invalid: {}'.format(name, result))

        return result

    async def current_op(self, include_all: bool = False) -> dict:
        """Get information on operations currently running. Works only for
        MongoDB >= 3.2

        :Parameters:
          - `include_all` (optional): if ``True`` also list currently
            idle operations in the result
        """
        connection = await self.client.get_connection()
        if connection.max_wire_version < 4:
            raise NotImplementedError('Only implemented from wired protocol version >= 4')

        cmd = SON([('currentOp', 1), ('$all', include_all)])
        return await connection.command(
            'admin', cmd, ReadPreference.PRIMARY, DEFAULT_CODEC_OPTIONS
        )

    async def profiling_level(self) -> int:
        """Get the database's current profiling level.

        Returns one of (:data:`~pymongo.OFF`,
        :data:`~pymongo.SLOW_ONLY`, :data:`~pymongo.ALL`).
        """

        result = await self.command('profile', value=-1)

        assert 0 <= result['was'] <= 2

        return result['was']

    async def set_profiling_level(self, level, slow_ms=None) -> None:
        """Set the database's profiling level.

        :Parameters:
          - `level`: Specifies a profiling level, see list of possible values
            below.
          - `slow_ms`: Optionally modify the threshold for the profile to
            consider a query or operation.  Even if the profiler is off queries
            slower than the `slow_ms` level will get written to the logs.

        Possible `level` values:

        +----------------------------+------------------------------------+
        | Level                      | Setting                            |
        +============================+====================================+
        | :data:`~pymongo.OFF`       | Off. No profiling.                 |
        +----------------------------+------------------------------------+
        | :data:`~pymongo.SLOW_ONLY` | On. Only includes slow operations. |
        +----------------------------+------------------------------------+
        | :data:`~pymongo.ALL`       | On. Includes all operations.       |
        +----------------------------+------------------------------------+

        Raises :class:`ValueError` if level is not one of
        (:data:`~pymongo.OFF`, :data:`~pymongo.SLOW_ONLY`,
        :data:`~pymongo.ALL`).

        .. mongodoc:: profiling
        """
        if not isinstance(level, int) or level < 0 or level > 2:
            raise ValueError('level must be one of (OFF, SLOW_ONLY, ALL)')

        if slow_ms is not None and not isinstance(slow_ms, int):
            raise TypeError('slow_ms must be an integer')

        if slow_ms is not None:
            await self.command('profile', level, slowms=slow_ms)
        else:
            await self.command('profile', level)

    async def profiling_info(self) -> List[dict]:
        """Returns a list containing current profiling information.

        .. mongodoc:: profiling
        """
        coll = self['system.profile']

        result = []
        async with coll.find() as cursor:
            async for item in cursor:
                result.append(item)

        return result

    def _default_role(self, read_only: bool) -> str:
        """Return the default user role for this database."""
        if self.name == 'admin':
            if read_only:
                return 'readAnyDatabase'
            else:
                return 'root'
        else:
            if read_only:
                return 'read'
            else:
                return 'dbOwner'

    async def _create_or_update_user(
            self, create: bool, name: str, password: str, read_only: bool, **kwargs) -> None:
        """Use a command to create (if create=True) or modify a user.
        """
        opts = {}
        if read_only or (create and 'roles' not in kwargs):
            warnings.warn('Creating a user with the read_only option '
                          'or without roles is deprecated in MongoDB '
                          '>= 2.6', DeprecationWarning)

            opts['roles'] = [self._default_role(read_only)]

        elif read_only:
            warnings.warn('The read_only option is deprecated in MongoDB '
                          '>= 2.6, use "roles" instead', DeprecationWarning)

        if password is not None:
            # We always salt and hash client side.
            if 'digestPassword' in kwargs:
                raise ConfigurationError('The digestPassword option is not '
                                         'supported via add_user. Please use '
                                         'db.command("createUser", ...) '
                                         'instead for this option.')
            opts['pwd'] = auth._password_digest(name, password)
            opts['digestPassword'] = False

        # Don't send {} as writeConcern.
        if self.write_concern.acknowledged and self.write_concern.document:
            opts['writeConcern'] = self.write_concern.document
        opts.update(kwargs)

        if create:
            command_name = 'createUser'
        else:
            command_name = 'updateUser'

        await self.command(command_name, value=name, **opts)

    async def add_user(self, name: str, password: Optional[str] = None, read_only: Optional[bool] = None, **kwargs):
        """Create user `name` with password `password`.

        Add a new user with permissions for this :class:`Database`.

        .. note:: Will change the password if user `name` already exists.

        :Parameters:
          - `name`: the name of the user to create
          - `password` (optional): the password of the user to create. Can not
            be used with the ``userSource`` argument.
          - `read_only` (optional): if ``True`` the user will be read only
          - `**kwargs` (optional): optional fields for the user document
            (e.g. ``userSource``, ``otherDBRoles``, or ``roles``). See
            `<http://docs.mongodb.org/manual/reference/privilege-documents>`_
            for more information.

        """
        if not isinstance(name, str):
            raise TypeError('name must be an instance of str')
        if password is not None:
            if not isinstance(password, str):
                raise TypeError('password must be an instance of str')
            if len(password) == 0:
                raise ValueError('password can\'t be empty')
        if read_only is not None:
            read_only = common.validate_boolean('read_only', read_only)
            if 'roles' in kwargs:
                raise ConfigurationError('Can not use '
                                         'read_only and roles together')

        try:
            uinfo = await self.command('usersInfo', value=name)
            # Create the user if not found in uinfo, otherwise update one.
            await self._create_or_update_user(
                (not uinfo['users']), name, password, read_only, **kwargs)
        except OperationFailure as exc:
            # Unauthorized. Attempt to create the user in case of
            # localhost exception.
            if exc.code == 13:
                await self._create_or_update_user(
                    True, name, password, read_only, **kwargs)
            else:
                raise

    async def remove_user(self, name: str) -> None:
        """Remove user `name` from this :class:`Database`.

        User `name` will no longer have permissions to access this
        :class:`Database`.

        :Parameters:
          - `name`: the name of the user to remove
        """
        cmd = SON([('dropUser', name)])
        # Don't send {} as writeConcern.
        if self.write_concern.acknowledged and self.write_concern.document:
            cmd['writeConcern'] = self.write_concern.document
        await self.command(cmd)

    async def dereference(self, dbref: DBRef, **kwargs) -> MutableMapping:
        """Dereference a :class:`~bson.dbref.DBRef`, getting the
        document it points to.

        Raises :class:`TypeError` if `dbref` is not an instance of
        :class:`~bson.dbref.DBRef`. Returns a document, or ``None`` if
        the reference does not point to a valid document.  Raises
        :class:`ValueError` if `dbref` has a database specified that
        is different from the current database.

        :Parameters:
          - `dbref`: the reference
          - `**kwargs` (optional): any additional keyword arguments
            are the same as the arguments to
            :meth:`~aiomongo.collection.Collection.find`.
        """
        if not isinstance(dbref, DBRef):
            raise TypeError('cannot dereference a {}'.format(type(dbref)))
        if dbref.database is not None and dbref.database != self.name:
            raise ValueError('trying to dereference a DBRef that points to '
                             'another database ({} not {})'.format(dbref.database, self.name))
        return await self[dbref.collection].find_one({'_id': dbref.id}, **kwargs)
