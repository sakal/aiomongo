from typing import Optional, Union

from bson.codec_options import CodecOptions
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import _ALL_READ_PREFERENCES
from pymongo.write_concern import WriteConcern

import aiomongo
from .collection import Collection


class Database:

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

    def __getattr__(self, collection_name: str):
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

