import asyncio
import logging
import struct
from typing import List, Optional, Union

from bson import DEFAULT_CODEC_OPTIONS
from bson.codec_options import CodecOptions
from bson.son import SON
from pymongo import common, helpers, message
from pymongo.client_options import ClientOptions
from pymongo.ismaster import IsMaster
from pymongo.errors import ConfigurationError, ProtocolError, ConnectionFailure
from pymongo.read_concern import DEFAULT_READ_CONCERN, ReadConcern
from pymongo.read_preferences import ReadPreference, _ALL_READ_PREFERENCES
from pymongo.server_type import SERVER_TYPE

from .auth import get_authenticator
from .utils import IncrementalSleeper

logger = logging.getLogger('aiomongo.connection')


INT_MAX = 2147483647


class Connection:

    def __init__(self, loop: asyncio.AbstractEventLoop, host: str, port: int,
                 options: ClientOptions):
        self.host = host
        self.port = port
        self.loop = loop
        self.reader = None
        self.writer = None
        self.read_loop_task = None
        self.is_mongos = False
        self.is_writable = False
        self.max_bson_size = common.MAX_BSON_SIZE
        self.max_message_size = common.MAX_MESSAGE_SIZE
        self.max_wire_version = 0
        self.max_write_batch_size = common.MAX_WRITE_BATCH_SIZE
        self.options = options
        self.slave_ok = False

        self.__connected = asyncio.Event(loop=loop)
        self.__request_id = 0
        self.__request_futures = {}
        self.__sleeper = IncrementalSleeper(loop)

    @classmethod
    async def create(cls, loop: asyncio.AbstractEventLoop, host: str, port: int,
                     options: ClientOptions) -> 'Connection':
        conn = cls(loop, host, port, options)
        await conn.connect()
        return conn

    async def connect(self) -> None:
        if self.host.startswith('/'):
            self.reader, self.writer = await asyncio.open_unix_connection(
                path=self.host, loop=self.loop
            )
        else:
            self.reader, self.writer = await asyncio.open_connection(
                host=self.host, port=self.port, loop=self.loop
            )

        if self.host.startswith('/'):
            endpoint = self.host
        else:
            endpoint = '{}:{}'.format(self.host, self.port)
        logger.debug('Established connection to {}'.format(endpoint))
        self.read_loop_task = asyncio.ensure_future(self.read_loop(), loop=self.loop)

        ismaster = IsMaster(await self.command(
            'admin', SON([('ismaster', 1)]), ReadPreference.PRIMARY, DEFAULT_CODEC_OPTIONS
        ))

        self.is_mongos = ismaster.server_type == SERVER_TYPE.Mongos
        self.max_wire_version = ismaster.max_wire_version
        if ismaster.max_bson_size:
            self.max_bson_size = ismaster.max_bson_size
        if ismaster.max_message_size:
            self.max_message_size = ismaster.max_message_size
        if ismaster.max_write_batch_size:
            self.max_write_batch_size = ismaster.max_write_batch_size
        self.is_writable = ismaster.is_writable

        self.slave_ok = not self.is_mongos and self.options.read_preference != ReadPreference.PRIMARY

        if self.options.credentials:
            await self._authenticate()

        # Notify waiters that connection has been established
        self.__connected.set()

    async def reconnect(self) -> None:
        while True:
            try:
                await self.connect()
            except Exception as e:
                logger.error('Failed to reconnect: {}'.format(e))
                await self.__sleeper.sleep()
            else:
                self.__sleeper.reset()
                return

    def gen_request_id(self) -> int:
        while self.__request_id in self.__request_futures:
            self.__request_id += 1
            if self.__request_id >= INT_MAX:
                self.__request_id = 0

        return self.__request_id

    async def perform_operation(self, operation) -> bytes:
        request_id = None

        # Because pymongo uses rand() function internally to generate request_id
        # there is a possibility that we have more than one in-flight request with
        # the same id. To avoid this we rerun get_message function that regenerates
        # query with new request id. In most cases this loop will run only once.
        while request_id is None or request_id in self.__request_futures:
            msg = operation.get_message(self.slave_ok, self.is_mongos, True)
            request_id, data, _ = self._split_message(msg)

        response_future = asyncio.Future(loop=self.loop)
        self.__request_futures[request_id] = response_future

        await self.send_message(data)

        return await response_future

    async def write_command(self, request_id: int, msg: bytes) -> dict:
        response_future = asyncio.Future(loop=self.loop)
        self.__request_futures[request_id] = response_future

        await self.send_message(msg)

        response_data = await response_future
        response = helpers._unpack_response(response_data)
        assert response['number_returned'] == 1

        result = response['data'][0]

        # Raises NotMasterError or OperationFailure.
        helpers._check_command_response(result)
        return result

    async def send_message(self, msg: bytes) -> None:
        self.writer.write(msg)
        await self.writer.drain()

    async def command(self, dbname: str, spec: SON,
                      read_preference: Optional[Union[_ALL_READ_PREFERENCES]] = None,
                      codec_options: Optional[CodecOptions] = None, check: bool = True,
                      allowable_errors: Optional[List[str]] = None, check_keys: bool = False,
                      max_bson_size: Optional[int] =None,
                      read_concern: ReadConcern = DEFAULT_READ_CONCERN):

        if self.max_wire_version < 4 and not read_concern.ok_for_legacy:
            raise ConfigurationError(
                'Read concern of level {} is not valid with max wire version of {}'.format(
                    read_concern.level, self.max_wire_version
                )
            )

        read_preference = read_preference or self.options.read_preference
        codec_options = codec_options or self.options.codec_options

        name = next(iter(spec))
        ns = dbname + '.$cmd'

        if read_preference != ReadPreference.PRIMARY:
            flags = 4
        else:
            flags = 0

        if self.is_mongos:
            spec = message._maybe_add_read_preference(spec, read_preference)
        if read_concern.level:
            spec['readConcern'] = read_concern.document

        request_id, msg, size = message.query(flags, ns, 0, -1, spec,
                                              None, codec_options, check_keys)

        if (max_bson_size is not None
                and size > max_bson_size + message._COMMAND_OVERHEAD):
            message._raise_document_too_large(
                name, size, max_bson_size + message._COMMAND_OVERHEAD)

        response_future = asyncio.Future()
        self.__request_futures[request_id] = response_future

        self.writer.write(msg)
        await self.writer.drain()

        response = await response_future

        unpacked = helpers._unpack_response(response, codec_options=codec_options)
        response_doc = unpacked['data'][0]
        if check:
            helpers._check_command_response(response_doc, None, allowable_errors)

        return response_doc

    async def _authenticate(self) -> None:
        authenticator = get_authenticator(
            self.options.credentials.mechanism
        )
        await authenticator(self.options.credentials, self)

    @staticmethod
    def _split_message(msg: tuple) -> tuple:
        """Return request_id, data, max_doc_size.

        :Parameters:
          - `message`: (request_id, data, max_doc_size) or (request_id, data)
        """
        if len(msg) == 3:
            return msg
        else:
            # get_more and kill_cursors messages don't include BSON documents.
            request_id, data = msg
            return request_id, data, 0

    async def read_loop(self):

        while True:
            try:
                await self._read_loop_step()
            except (EOFError, ProtocolError) as e:
                self.__connected.clear()
                connection_error = ConnectionFailure('Connection was lost due to: {}'.format(str(e)))
                self.close(error=connection_error)
                for ft in self.__request_futures.values():
                    ft.set_exception(connection_error)
                self.__request_futures = {}
                await self.reconnect()
                return
            except asyncio.CancelledError:
                connection_error = ConnectionFailure('Shutting down.')
                for ft in self.__request_futures.values():
                    ft.set_exception(connection_error)
                return

    async def _read_loop_step(self):
        header = await self.reader.readexactly(16)
        length, = struct.unpack('<i', header[:4])
        if length < 16:
            raise ProtocolError('Message length ({}) not longer than standard '
                                'message header size (16)'.format(length))

        response_id, = struct.unpack('<i', header[8:12])

        if response_id not in self.__request_futures:
            raise ProtocolError(
                'Got response id {} but expected but request with such id was not sent.'.format(response_id)
            )
        message_data = await self.reader.readexactly(length - 16)

        ft = self.__request_futures.pop(response_id)
        if not ft.cancelled():
            ft.set_result(message_data)

    async def wait_connected(self) -> None:
        """Returns when connection is ready to be used"""
        await self.__connected.wait()

    def close(self, error: Optional[Exception] = None) -> None:
        if error is not None:
            logger.error(str(error))
        elif self.read_loop_task is not None:
            self.read_loop_task.cancel()

        self.writer.close()
