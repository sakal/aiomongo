import collections
import io
import struct
from typing import Iterable, Optional, Union, List, Tuple, MutableMapping

from bson import BSON, ObjectId
from bson.son import SON
from pymongo import common, message
from pymongo.errors import BulkWriteError, InvalidOperation
from pymongo.helpers import _check_write_command_response, _index_document
from pymongo.read_preferences import ReadPreference
from pymongo.results import BulkWriteResult, InsertManyResult, InsertOneResult
from pymongo.write_concern import WriteConcern

from .bulk import Bulk
from .cursor import Cursor


class Collection:

    def __init__(self, database, name, read_preference=None, read_concern=None, codec_options=None,
                 write_concern=None):
        self.database = database
        self.read_preference = read_preference or database.read_preference
        self.read_concern = read_concern or database.read_concern
        self.write_concern = write_concern or database.write_concern
        self.codec_options = codec_options or database.codec_options
        self.name = name

        self.__write_response_codec_options = self.codec_options._replace(
            unicode_decode_error_handler='replace',
            document_class=dict)

    def __str__(self):
        return '{}.{}'.format(self.database.name, self.name)

    def __repr__(self) -> str:
        return 'Collection({}, {})'.format(self.database.name, self.name)

    async def count(self, filter: Optional[dict]=None, hint: Optional[Union[str, List[Tuple]]]=None,
                    limit: Optional[int]=None, skip: Optional[int]=None, max_time_ms: Optional[int]=None) -> int:
        cmd = SON([('count', self.name)])
        if filter is not None:
            cmd['query'] = filter
        if hint is not None and not isinstance(hint, str):
            cmd['hint'] = _index_document(hint)
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

    async def find(self, filter: Optional[dict] = None, projection: Optional[Union[dict, list]] = None,
                   skip: int = 0, limit: int = 0, sort: Optional[List[Tuple]]=None, modifiers: Optional[dict]=None,
                   batch_size: int=100) -> Cursor:
        connection = self.database.client.get_connection()

        return Cursor(connection, self, filter, projection, skip, limit, sort, modifiers, batch_size)

    async def find_one(self, filter: Optional[dict] = None, projection: Optional[Union[dict, list]] = None,
                       skip: int = 0, sort: Optional[List[Tuple]]=None, modifiers: Optional[dict]=None) -> Optional[dict]:
        if isinstance(filter, ObjectId):
            filter = {'_id': filter}

        result_cursor = await self.find(
            filter=filter, projection=projection, skip=skip, limit=1, sort=sort, modifiers=modifiers
        )
        result = None
        async for item in result_cursor:
            result = item

        return result

    async def insert_one(self, document: MutableMapping, bypass_document_validation: bool=False,
                         check_keys: bool=True) -> InsertOneResult:
        if '_id' not in document:
            document['_id'] = ObjectId()

        write_concern = self.write_concern.document
        acknowledged = write_concern.get('w') != 0

        connection = self.database.client.get_connection()

        if acknowledged:
            command = SON([('insert', self.name),
                           ('ordered', True),
                           ('documents', [document])])

            if bypass_document_validation:
                command['bypassDocumentValidation'] = True

            result = await connection.command(
                self.database.name, command, ReadPreference.PRIMARY, self.codec_options
            )

            _check_write_command_response([(0, result)])
        else:
            _, msg, _ = message.insert(
                str(self), [document], check_keys,
                acknowledged, write_concern, False, self.__write_response_codec_options
            )
            await connection.send_message(msg)

        return InsertOneResult(document['_id'], acknowledged)

    async def insert_many(self, documents: Iterable[dict], ordered: bool=True,
                          bypass_document_validation: bool=False) -> InsertManyResult:

        if not isinstance(documents, collections.Iterable) or not documents:
            raise TypeError("documents must be a non-empty list")

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

