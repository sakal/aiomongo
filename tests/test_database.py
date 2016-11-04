import datetime
import re

import pytest
from bson import DBRef, ObjectId
from bson.codec_options import CodecOptions
from bson.int64 import Int64
from bson.regex import Regex
from bson.son import SON
from pymongo import ALL, OFF, SLOW_ONLY
from pymongo.errors import OperationFailure
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern

from aiomongo import Database, Collection


class TestDatabase:

    @pytest.mark.asyncio
    async def test_command(self, mongo):

        db = mongo.admin
        assert await db.command('buildinfo') == await db.command({'buildinfo': 1})

    @pytest.mark.asyncio
    async def test_get_collection(self, test_db):
        codec_options = CodecOptions(tz_aware=True)
        write_concern = WriteConcern(w=2, j=True)
        read_concern = ReadConcern('majority')
        coll = test_db.get_collection(
            'foo', codec_options, ReadPreference.SECONDARY, write_concern,
            read_concern)
        assert 'foo' == coll.name
        assert codec_options == coll.codec_options
        assert ReadPreference.SECONDARY == coll.read_preference
        assert write_concern == coll.write_concern
        assert read_concern == coll.read_concern

    @pytest.mark.asyncio
    async def test_getattr(self, test_db):
        assert isinstance(test_db['_does_not_exist'], Collection)

        with pytest.raises(AttributeError) as context:
            test_db._does_not_exist

        # Message should be: "AttributeError: Database has no attribute
        # '_does_not_exist'. To access the _does_not_exist collection,
        # use database['_does_not_exist']".
        assert ('has no attribute _does_not_exist' in str(context.value))

    @pytest.mark.asyncio
    async def test_equality(self, mongo):
        assert Database(mongo, 'test') != Database(mongo, 'mike')
        assert Database(mongo, 'test') == Database(mongo, 'test')

        # Explicitly test inequality
        assert not (Database(mongo, 'test') != Database(mongo, 'test'))

    @pytest.mark.asyncio
    async def test_get_coll(self, test_db):
        assert test_db.test == test_db['test']
        assert test_db.test == Collection(test_db, 'test')
        assert test_db.test != Collection(test_db, 'mike')
        assert test_db.test.mike == test_db['test.mike']

    @pytest.mark.asyncio
    async def test_collection_names(self, mongo, test_db):
        await test_db.test.insert_one({'dummy': 'object'})
        await test_db.test.mike.insert_one({'dummy': 'object'})

        colls = await test_db.collection_names()
        assert 'test' in colls
        assert 'test.mike' in colls
        for coll in colls:
            assert '$' not in coll

        colls_without_systems = await test_db.collection_names(False)
        for coll in colls_without_systems:
            assert not coll.startswith('system.')

        # Force more than one batch.
        db = mongo.many_collections
        for i in range(101):
            await db['coll' + str(i)].insert_one({})
        # No Error
        try:
            await db.collection_names()
        finally:
            await mongo.drop_database('many_collections')

    @pytest.mark.asyncio
    async def test_drop_collection(self, test_db):
        with pytest.raises(TypeError):
            await test_db.drop_collection(5)
        with pytest.raises(TypeError):
            await test_db.drop_collection(None)

        await test_db.test.insert_one({'dummy': 'object'})
        assert 'test' in (await test_db.collection_names())
        await test_db.drop_collection('test')
        assert 'test' not in (await test_db.collection_names())

        await test_db.test.insert_one({'dummy': 'object'})
        assert 'test' in (await test_db.collection_names())
        await test_db.drop_collection('test')
        assert 'test' not in (await test_db.collection_names())

        await test_db.test.insert_one({'dummy': 'object'})
        assert 'test' in (await test_db.collection_names())
        await test_db.drop_collection('test')
        assert 'test' not in (await test_db.collection_names())

        await test_db.test.insert_one({'dummy': 'object'})
        assert 'test' in (await test_db.collection_names())
        await test_db.drop_collection('test')
        assert 'test' not in (await test_db.collection_names())

        await test_db.drop_collection(test_db.test.doesnotexist)

    @pytest.mark.asyncio
    async def test_validate_collection(self, test_db):
        with pytest.raises(TypeError):
            await test_db.validate_collection(5)
        with pytest.raises(TypeError):
            await test_db.validate_collection(None)

        await test_db.test.insert_one({'dummy': 'object'})

        with pytest.raises(OperationFailure):
            await test_db.validate_collection('test.doesnotexist"')
        with pytest.raises(OperationFailure):
            await test_db.validate_collection(test_db.test.doesnotexist)

        assert await test_db.validate_collection('test')
        assert await test_db.validate_collection(test_db.test)
        assert await test_db.validate_collection(test_db.test, full=True)
        assert await test_db.validate_collection(test_db.test, scandata=True)
        assert await test_db.validate_collection(test_db.test, full=True, scandata=True)
        assert await test_db.validate_collection(test_db.test, True, True)

    @pytest.mark.asyncio
    async def test_profiling_levels(self, mongo, test_db):

        connection = await mongo.get_connection()
        if connection.is_mongos:
            pytest.skip('Profiling works only without mongos.')
            return

        assert await test_db.profiling_level() == OFF  # default

        with pytest.raises(ValueError):
            await test_db.set_profiling_level(5.5)
        with pytest.raises(ValueError):
            await test_db.set_profiling_level(None)
        with pytest.raises(ValueError):
            await test_db.set_profiling_level(-1)
        with pytest.raises(TypeError):
            await test_db.set_profiling_level(SLOW_ONLY, 5.5)
        with pytest.raises(TypeError):
            await test_db.set_profiling_level(SLOW_ONLY, '1')

        await test_db.set_profiling_level(SLOW_ONLY)
        assert await test_db.profiling_level() == SLOW_ONLY

        await test_db.set_profiling_level(ALL)
        assert await test_db.profiling_level() == ALL

        await test_db.set_profiling_level(OFF)
        assert await test_db.profiling_level() == OFF

        await test_db.set_profiling_level(SLOW_ONLY, 50)
        assert 50 == (await test_db.command('profile', -1))['slowms']

        await test_db.set_profiling_level(ALL, -1)
        assert -1 == (await test_db.command('profile', -1))['slowms']

        await test_db.set_profiling_level(OFF, 100)  # back to default
        assert 100 == (await test_db.command('profile', -1))['slowms']

    @pytest.mark.asyncio
    async def test_profiling_info(self, mongo, test_db):
        connection = await mongo.get_connection()
        if connection.is_mongos:
            pytest.skip('Profiling works only without mongos.')
            return

        await test_db.system.profile.drop()
        await test_db.set_profiling_level(ALL)
        await test_db.test.find_one()
        await test_db.set_profiling_level(OFF)

        info = await test_db.profiling_info()
        assert isinstance(info, list)

        assert len(info) >= 1
        # These basically clue us in to server changes.
        assert isinstance(info[0]['responseLength'], int)
        assert isinstance(info[0]['millis'], int)
        assert isinstance(info[0]['client'], str)
        assert isinstance(info[0]['user'], str)
        assert isinstance(info[0]['ns'], str)
        assert isinstance(info[0]['op'], str)
        assert isinstance(info[0]['ts'], datetime.datetime)

    @pytest.mark.asyncio
    async def test_command_with_regex(self, test_db):
        await test_db.test.insert_one({'r': re.compile('.*')})
        await test_db.test.insert_one({'r': Regex('.*')})

        result = await test_db.command('aggregate', 'test', pipeline=[])
        for doc in result['result']:
            assert isinstance(doc['r'], Regex)

    @pytest.mark.asyncio
    async def test_deref(self, test_db):
        with pytest.raises(TypeError):
            await test_db.dereference(5)
        with pytest.raises(TypeError):
            await test_db.dereference('hello')
        with pytest.raises(TypeError):
            await test_db.dereference(None)

        assert await test_db.dereference(DBRef("test", ObjectId())) is None
        obj = {'x': True}
        key = (await test_db.test.insert_one(obj)).inserted_id
        assert await test_db.dereference(DBRef('test', key)) == obj
        assert await test_db.dereference(DBRef('test', key, 'aiomongo_test')) == obj

        with pytest.raises(ValueError):
            await test_db.dereference(DBRef('test', key, 'foo'))

        assert await test_db.dereference(DBRef('test', 4)) is None
        obj = {'_id': 4}
        await test_db.test.insert_one(obj)
        assert await test_db.dereference(DBRef('test', 4)) == obj

    @pytest.mark.asyncio
    async def test_deref_kwargs(self, mongo, test_db):
        await test_db.test.insert_one({'_id': 4, 'foo': 'bar'})
        db = mongo.get_database(
            'aiomongo_test', codec_options=CodecOptions(document_class=SON))
        assert SON([('foo', 'bar')]) == await db.dereference(DBRef('test', 4), projection={'_id': False})

    @pytest.mark.asyncio
    async def test_insert_find_one(self, test_db):
        a_doc = SON({'hello': 'world'})
        a_key = (await test_db.test.insert_one(a_doc)).inserted_id
        assert isinstance(a_doc['_id'], ObjectId)
        assert a_doc['_id'] == a_key
        assert a_doc == await test_db.test.find_one({'_id': a_doc['_id']})
        assert a_doc == await test_db.test.find_one(a_key)
        assert await test_db.test.find_one(ObjectId()) is None
        assert a_doc == await test_db.test.find_one({'hello': 'world'})
        assert await test_db.test.find_one({'hello': 'test'}) is None

        b = await test_db.test.find_one()
        b['hello'] = 'mike'
        await test_db.test.replace_one({'_id': b['_id']}, b)

        assert a_doc != await test_db.test.find_one(a_key)
        assert b == await test_db.test.find_one(a_key)
        assert b == await test_db.test.find_one()

        count = 0

        async with test_db.test.find() as cursor:
            async for _ in cursor:
                count += 1
        assert count == 1

    @pytest.mark.asyncio
    async def test_long(self, test_db):
        await test_db.test.insert_one({'x': 9223372036854775807})
        retrieved = (await test_db.test.find_one())['x']
        assert Int64(9223372036854775807) == retrieved
        assert isinstance(retrieved, Int64)
        await test_db.test.delete_many({})
        await test_db.test.insert_one({'x': Int64(1)})
        retrieved = (await test_db.test.find_one())['x']
        assert Int64(1) == retrieved
        assert isinstance(retrieved, Int64)

    @pytest.mark.asyncio
    async def test_delete(self, test_db):
        await test_db.test.insert_one({'x': 1})
        await test_db.test.insert_one({'x': 2})
        await test_db.test.insert_one({'x': 3})
        length = 0
        async with test_db.test.find() as cursor:
            async for _ in cursor:
                length += 1
        assert length == 3

        await test_db.test.delete_one({'x': 1})
        length = 0
        async with test_db.test.find() as cursor:
            async for _ in cursor:
                length += 1
        assert length == 2

        await test_db.test.delete_one(await test_db.test.find_one())
        await test_db.test.delete_one(await test_db.test.find_one())
        assert await test_db.test.find_one() is None

        await test_db.test.insert_one({'x': 1})
        await test_db.test.insert_one({'x': 2})
        await test_db.test.insert_one({'x': 3})

        assert await test_db.test.find_one({'x': 2})
        await test_db.test.delete_one({'x': 2})
        assert not await test_db.test.find_one({'x': 2})

        assert await test_db.test.find_one()
        await test_db.test.delete_many({})
        assert not await test_db.test.find_one()

