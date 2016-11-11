import struct
from typing import Iterable, Iterator, List, Tuple

from bson import BSON, ObjectId
from bson.codec_options import CodecOptions
from bson.son import SON
from io import BytesIO
from pymongo.bulk import _COMMANDS, _Run, _merge_command
from pymongo.common import (validate_is_document_type,
                            validate_ok_for_replace,
                            validate_ok_for_update)
from pymongo.errors import BulkWriteError, InvalidOperation
from pymongo.message import (_COMMAND_OVERHEAD, _INSERT, _UPDATE, _DELETE, _BSONOBJ,
                             _ZERO_8, _ZERO_16, _ZERO_32, _ZERO_64, _SKIPLIM,
                             _OP_MAP, _raise_document_too_large)
from pymongo.write_concern import WriteConcern


import aiomongo


class Bulk:

    def __init__(self, collection: 'aiomongo.Collection', ordered: bool, bypass_document_validation: bool):
        self.collection = collection
        self.ordered = ordered
        self.ops = []
        self.name = '{}.{}'.format(collection.database.name, collection.name)
        self.namespace = collection.database.name + '.$cmd'
        self.executed = False
        self.bypass_doc_val = bypass_document_validation

    def add_insert(self, document: dict) -> None:
        """Add an insert document to the list of ops.
        """
        validate_is_document_type('document', document)
        # Generate ObjectId client side.
        if '_id' not in document:
            document['_id'] = ObjectId()
        self.ops.append((_INSERT, document))

    def add_update(self, selector: dict, update: dict, multi: bool = False, upsert: bool = False) -> None:
        """Create an update document and add it to the list of ops.
        """
        validate_ok_for_update(update)
        cmd = SON([('q', selector), ('u', update),
                   ('multi', multi), ('upsert', upsert)])
        self.ops.append((_UPDATE, cmd))

    def add_replace(self, selector: dict, replacement: dict, upsert: bool=False) -> None:
        """Create a replace document and add it to the list of ops.
        """
        validate_ok_for_replace(replacement)
        cmd = SON([('q', selector), ('u', replacement),
                   ('multi', False), ('upsert', upsert)])
        self.ops.append((_UPDATE, cmd))

    def add_delete(self, selector: dict, limit: int) -> None:
        """Create a delete document and add it to the list of ops.
        """
        cmd = SON([('q', selector), ('limit', limit)])
        self.ops.append((_DELETE, cmd))

    def gen_ordered(self) -> Iterator[_Run]:
        """Generate batches of operations, batched by type of
        operation, in the order **provided**.
        """
        run = None
        for idx, (op_type, operation) in enumerate(self.ops):
            if run is None:
                run = _Run(op_type)
            elif run.op_type != op_type:
                yield run
                run = _Run(op_type)
            run.add(idx, operation)
        yield run

    def gen_unordered(self) -> Iterator[_Run]:
        """Generate batches of operations, batched by type of
        operation, in arbitrary order.
        """
        operations = [_Run(_INSERT), _Run(_UPDATE), _Run(_DELETE)]
        for idx, (op_type, operation) in enumerate(self.ops):
            operations[op_type].add(idx, operation)

        for run in operations:
            if run.ops:
                yield run

    async def execute_command(self, connection: 'aiomongo.Connection', generator: Iterable[_Run],
                              write_concern: WriteConcern) -> dict:
        """Execute using write commands.
        """
        # nModified is only reported for write commands, not legacy ops.
        full_result = {
            'writeErrors': [],
            'writeConcernErrors': [],
            'nInserted': 0,
            'nUpserted': 0,
            'nMatched': 0,
            'nModified': 0,
            'nRemoved': 0,
            'upserted': [],
        }

        for run in generator:
            cmd = SON([(_COMMANDS[run.op_type], self.collection.name),
                       ('ordered', self.ordered)])
            if write_concern.document:
                cmd['writeConcern'] = write_concern.document
            if self.bypass_doc_val and connection.max_wire_version >= 4:
                cmd['bypassDocumentValidation'] = True

            results = await self._do_batched_write_command(
                self.namespace, run.op_type, cmd,
                run.ops, True, self.collection.codec_options, connection)

            _merge_command(run, full_result, results)
            # We're supposed to continue if errors are
            # at the write concern level (e.g. wtimeout)
            if self.ordered and full_result['writeErrors']:
                break

        if full_result['writeErrors'] or full_result['writeConcernErrors']:
            if full_result['writeErrors']:
                full_result['writeErrors'].sort(
                    key=lambda error: error['index'])
            raise BulkWriteError(full_result)
        return full_result

    async def execute_no_results(self, connection: 'aiomongo.Connection', generator: Iterable[_Run]) -> dict:
        raise NotImplemented('Should be implemented in future versions of aiomongo.')

    async def execute(self, write_concern: dict) -> dict:
        """Execute operations.
        """
        if not self.ops:
            raise InvalidOperation('No operations to execute')
        if self.executed:
            raise InvalidOperation('Bulk operations can '
                                   'only be executed once.')

        self.executed = True
        write_concern = (WriteConcern(**write_concern) if
                         write_concern else self.collection.write_concern)

        if self.ordered:
            generator = self.gen_ordered()
        else:
            generator = self.gen_unordered()

        connection = await self.collection.database.client.get_connection()

        if not write_concern.acknowledged:
            await self.execute_no_results(connection, generator)
        else:
            return await self.execute_command(connection, generator, write_concern)

    async def _do_batched_write_command(self, namespace: str, operation: str, command: SON,
                                        docs: Iterable[dict], check_keys: bool, opts: CodecOptions,
                                        connection) -> List[Tuple[int, dict]]:

        # Max BSON object size + 16k - 2 bytes for ending NUL bytes.
        # Server guarantees there is enough room: SERVER-10643.
        max_cmd_size = connection.max_bson_size + _COMMAND_OVERHEAD

        ordered = command.get('ordered', True)

        buf = BytesIO()

        # Save space for message length and request id
        buf.write(_ZERO_64)
        # responseTo, opCode
        buf.write(b'\x00\x00\x00\x00\xd4\x07\x00\x00')
        # No options
        buf.write(_ZERO_32)
        # Namespace as C string
        buf.write(namespace.encode())
        buf.write(_ZERO_8)
        # Skip: 0, Limit: -1
        buf.write(_SKIPLIM)

        # Where to write command document length
        command_start = buf.tell()
        buf.write(BSON.encode(command))
        # Start of payload
        buf.seek(-1, 2)
        # Work around some Jython weirdness.
        buf.truncate()

        try:
            buf.write(_OP_MAP[operation])
        except KeyError:
            raise InvalidOperation('Unknown command')

        if operation in (_UPDATE, _DELETE):
            check_keys = False

        # Where to write list document length
        list_start = buf.tell() - 4

        # If there are multiple batches we'll
        # merge results in the caller.
        results = []

        idx = 0
        idx_offset = 0
        has_docs = False

        for doc in docs:
            has_docs = True
            key = str(idx).encode()
            value = BSON.encode(doc, check_keys, opts)

            # Send a batch?
            enough_data = (buf.tell() + len(key) + len(value) + 2) >= max_cmd_size
            enough_documents = (idx >= connection.max_write_batch_size)
            if enough_data or enough_documents:
                if not idx:
                    write_op = 'insert' if operation == _INSERT else None
                    _raise_document_too_large(
                        write_op, len(value), connection.max_bson_size)
                result = await self._send_message(connection, buf, command_start, list_start)
                results.append((idx_offset, result))
                if ordered and 'writeErrors' in result:
                    return results

                # Truncate back to the start of list elements
                buf.seek(list_start + 4)
                buf.truncate()
                idx_offset += idx
                idx = 0
                key = b'0'

            buf.write(_BSONOBJ)
            buf.write(key)
            buf.write(_ZERO_8)
            buf.write(value)
            idx += 1

        if not has_docs:
            raise InvalidOperation("cannot do an empty bulk write")

        result = await self._send_message(connection, buf, command_start, list_start)
        results.append((idx_offset, result))
        return results

    @staticmethod
    async def _send_message(connection: 'aiomongo.Connection', buf: BytesIO, command_start: int,
                            list_start: int) -> dict:

        # Close list and command documents
        buf.write(_ZERO_16)

        # Write document lengths and request id
        length = buf.tell()
        buf.seek(list_start)
        buf.write(struct.pack('<i', length - list_start - 1))
        buf.seek(command_start)
        buf.write(struct.pack('<i', length - command_start))
        buf.seek(4)
        request_id = connection.gen_request_id()
        buf.write(struct.pack('<i', request_id))
        buf.seek(0)
        buf.write(struct.pack('<i', length))
        return await connection.write_command(request_id, buf.getvalue())
