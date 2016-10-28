import asyncio
import logging
import struct

from bson import DEFAULT_CODEC_OPTIONS
from pymongo import common, helpers, message
from pymongo.ismaster import IsMaster
from pymongo.errors import ConfigurationError, ProtocolError
from pymongo.read_concern import DEFAULT_READ_CONCERN
from pymongo.read_preferences import ReadPreference
from pymongo.server_type import SERVER_TYPE

logger = logging.getLogger('aiomongo.connection')


INT_MAX = 2147483647


class Connection:

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        self.read_loop_task = None
        self.is_mongos = False
        self.is_writable = False
        self.max_bson_size = common.MAX_BSON_SIZE
        self.max_message_size = common.MAX_MESSAGE_SIZE
        self.max_wire_version = 0
        self.max_write_batch_size = common.MAX_WRITE_BATCH_SIZE
        self.slave_ok = False

        self.__request_id = 0
        self.__request_futures = {}

    @classmethod
    async def create(cls, loop, host: str, port: int=27017, read_preference=ReadPreference.PRIMARY):
        reader, writer = await asyncio.open_connection(host=host, port=port, loop=loop)
        logging.debug('Established connection to {}:{}'.format(host, port))
        conn = cls(reader, writer)
        conn.read_loop_task = asyncio.ensure_future(conn.read_loop(), loop=loop)

        ismaster = IsMaster(await conn.command(
            'admin', {'ismaster': 1}, ReadPreference.PRIMARY, DEFAULT_CODEC_OPTIONS
        ))

        conn.is_mongos = ismaster.server_type == SERVER_TYPE.Mongos
        conn.max_wire_version = ismaster.max_wire_version
        if ismaster.max_bson_size:
            conn.max_bson_size = ismaster.max_bson_size
        if ismaster.max_message_size:
            conn.max_message_size = ismaster.max_message_size
        if ismaster.max_write_batch_size:
            conn.max_write_batch_size = ismaster.max_write_batch_size
        conn.is_writable = ismaster.is_writable

        conn.slave_ok = not conn.is_mongos and read_preference != ReadPreference.PRIMARY

        return conn

    def gen_request_id(self) -> int:
        while self.__request_id in self.__request_futures:
            self.__request_id += 1
            if self.__request_id >= INT_MAX:
                self.__request_id = 0

        return self.__request_id

    async def perform_operation(self, operation) -> bytes:
        message = operation.get_message(self.slave_ok, self.is_mongos, True)

        request_id, data, _ = self._split_message(message)
        response_future = asyncio.Future()
        self.__request_futures[request_id] = response_future

        await self.send_message(data)

        return await response_future

    async def write_command(self, request_id: int, message: bytes) -> dict:
        response_future = asyncio.Future()
        self.__request_futures[request_id] = response_future

        await self.send_message(message)

        response_data = await response_future
        response = helpers._unpack_response(response_data)
        assert response['number_returned'] == 1

        result = response['data'][0]

        # Raises NotMasterError or OperationFailure.
        helpers._check_command_response(result)
        return result

    async def send_message(self, message: bytes) -> None:
        self.writer.write(message)
        await self.writer.drain()

    async def command(self, dbname, spec, read_preference, codec_options, check=True,
                      allowable_errors=None, check_keys=False, max_bson_size=None,
                      read_concern=DEFAULT_READ_CONCERN):

        if self.max_wire_version < 4 and not read_concern.ok_for_legacy:
            raise ConfigurationError(
                'Read concern of level {} is not valid with max wire version of {}'.format(
                    read_concern.level, self.max_wire_version
                )
            )

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

    def _split_message(self, message):
        """Return request_id, data, max_doc_size.

        :Parameters:
          - `message`: (request_id, data, max_doc_size) or (request_id, data)
        """
        if len(message) == 3:
            return message
        else:
            # get_more and kill_cursors messages don't include BSON documents.
            request_id, data = message
            return request_id, data, 0

    async def read_loop(self):

        while True:
            try:
                await self._read_loop_step()
            except (EOFError, asyncio.CancelledError, ProtocolError) as e:
                logging.debug('Closing connection due to error: {}'.format(e))
                self.close()
                for ft in self.__request_futures.values():
                    ft.set_exception(e)
                self.__request_futures = {}
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

    def close(self):
        if self.read_loop_task is not None:
            self.read_loop_task.cancel()

        self.writer.close()
