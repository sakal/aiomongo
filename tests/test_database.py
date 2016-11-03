import datetime

import pytest
from bson.codec_options import CodecOptions
from pymongo import ALL, OFF, SLOW_ONLY
from pymongo.errors import OperationFailure
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern

from aiomongo import Database, Collection


class TestDatabase:

    @pytest.mark.asyncio
    async def test_command_buildinfo(self, test_db):

        build_info = await test_db.command('buildinfo')
        assert build_info.get('ok') == 1.0

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