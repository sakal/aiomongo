from bson.son import SON
from pymongo.helpers import _check_command_response

from .collection import Collection


class Database:

    def __init__(self, client, name: str, read_preference=None, read_concern=None, codec_options=None,
                 write_concern=None):
        self.client = client
        self.name = name
        self.options = client.options
        self.read_preference = read_preference or self.options.read_preference
        self.read_concern = read_concern or self.options.read_concern
        self.codec_options = codec_options or self.options.codec_options
        self.write_concern = write_concern or self.options.write_concern

    def __getitem__(self, collection_name):
        return Collection(self, collection_name)

    def __getattr__(self, collection_name):
        return self[collection_name]

    def __repr__(self) -> str:
        return 'Database({})'.format(self.name)

    def __str__(self) -> str:
        return self.name

    async def command(self, command, value=1, check=True, allowable_errors=None, **kwargs):
        if isinstance(command, (bytes, str)):
            command = SON([(command, value)])

        collection = self['$cmd']
        response = await collection.find_one(command, **kwargs)

        if check:
            msg = "AioMongo: command {0} on namespace {1} failed with '%s'".format(repr(command), collection)
            _check_command_response(response, msg, allowable_errors)

        return response
