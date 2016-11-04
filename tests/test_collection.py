import asyncio
import re
from collections import defaultdict

import pytest
from bson import ObjectId, BSON, Code, Regex
from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument
from bson.son import SON
from pymongo import (ASCENDING, DESCENDING, GEO2D,
                     GEOHAYSTACK, GEOSPHERE, HASHED, TEXT,
                     ReturnDocument)
from pymongo.errors import (BulkWriteError, DocumentTooLarge, DuplicateKeyError, InvalidDocument,
                            InvalidName, InvalidOperation, OperationFailure)
from pymongo.message import _COMMAND_OVERHEAD
from pymongo.operations import *
from pymongo.results import (InsertOneResult,
                             InsertManyResult,
                             UpdateResult,
                             DeleteResult)
from pymongo.write_concern import WriteConcern

from aiomongo import Collection
from aiomongo.command_cursor import CommandCursor


class TestCollection:

    @pytest.mark.asyncio
    async def test_equality(self, test_db):
        assert isinstance(test_db.test, Collection)
        assert test_db.test == test_db['test']
        assert test_db.test == Collection(test_db, 'test')
        assert test_db.test.mike == test_db['test.mike']
        assert test_db.test['mike'], test_db['test.mike']

    @pytest.mark.asyncio
    async def test_drop_nonexistent_collection(self, test_db):
        await test_db.drop_collection('test')
        assert 'test' not in await test_db.collection_names()

        # No exception
        await test_db.drop_collection('test')

    @pytest.mark.asyncio
    async def test_create_indexes(self, test_db):

        with pytest.raises(TypeError):
            await test_db.test.create_indexes('foo')
        with pytest.raises(TypeError):
            await test_db.test.create_indexes(['foo'])

        await test_db.test.drop_indexes()
        await test_db.test.insert_one({})
        assert len(await test_db.test.index_information()) == 1

        await test_db.test.create_indexes([IndexModel('hello')])
        await test_db.test.create_indexes([IndexModel([('hello', DESCENDING),
                                           ('world', ASCENDING)])])

        # Tuple instead of list.
        await test_db.test.create_indexes([IndexModel((('world', ASCENDING),))])

        assert len(await test_db.test.index_information()) == 4

        await test_db.test.drop_indexes()
        names = await test_db.test.create_indexes([IndexModel([('hello', DESCENDING),
                                                   ('world', ASCENDING)],
                                                   name='hello_world')])
        assert names == ['hello_world']

        await test_db.test.drop_indexes()
        assert len(await test_db.test.index_information()) == 1
        await test_db.test.create_indexes([IndexModel('hello')])
        assert 'hello_1' in await test_db.test.index_information()

        await test_db.test.drop_indexes()
        assert len(await test_db.test.index_information()) == 1
        names = await test_db.test.create_indexes([IndexModel([('hello', DESCENDING),
                                                   ('world', ASCENDING)]),
                                                  IndexModel('hello')])
        info = await test_db.test.index_information()
        for name in names:
            assert name in info

        await test_db.test.drop()
        await test_db.test.insert_one({'a': 1})
        await test_db.test.insert_one({'a': 1})
        with pytest.raises(DuplicateKeyError):
            await test_db.test.create_indexes([IndexModel('a', unique=True)])

    @pytest.mark.asyncio
    async def test_create_index(self, test_db):
        with pytest.raises(TypeError):
            await test_db.test.create_indexe(5)
        with pytest.raises(TypeError):
            await test_db.test.create_indexe({'hello': 1})
        with pytest.raises(ValueError):
            await test_db.test.create_index([])

        await test_db.test.drop_indexes()
        await test_db.test.insert_one({})
        assert len(await test_db.test.index_information()) == 1

        await test_db.test.create_index('hello')
        await test_db.test.create_index([('hello', DESCENDING), ('world', ASCENDING)])

        # Tuple instead of list.
        await test_db.test.create_index((('world', ASCENDING),))

        assert len(await test_db.test.index_information()) == 4

        await test_db.test.drop_indexes()
        ix = await test_db.test.create_index(
            [('hello', DESCENDING), ('world', ASCENDING)], name='hello_world'
        )
        assert ix == 'hello_world'

        await test_db.test.drop_indexes()
        assert len(await test_db.test.index_information()) == 1
        await test_db.test.create_index('hello')
        assert 'hello_1' in await test_db.test.index_information()

        await test_db.test.drop_indexes()
        assert len(await test_db.test.index_information()) == 1
        await test_db.test.create_index([('hello', DESCENDING), ('world', ASCENDING)])
        assert 'hello_-1_world_1' in await test_db.test.index_information()

        await test_db.test.drop()
        await test_db.test.insert_one({'a': 1})
        await test_db.test.insert_one({'a': 1})
        with pytest.raises(DuplicateKeyError):
            await test_db.test.create_index('a', unique=True)

    @pytest.mark.asyncio
    async def test_drop_index(self, test_db):
        await test_db.test.create_index('hello')
        name = await test_db.test.create_index('goodbye')

        assert len(await test_db.test.index_information()) == 3
        assert name == 'goodbye_1'
        await test_db.test.drop_index(name)

        # Drop it again.
        with pytest.raises(OperationFailure):
            await test_db.test.drop_index(name)
        assert len(await test_db.test.index_information()) == 2
        assert 'hello_1' in await test_db.test.index_information()

        await test_db.test.drop_indexes()
        await test_db.test.create_index('hello')
        name = await test_db.test.create_index('goodbye')

        assert len(await test_db.test.index_information()) == 3
        assert name == 'goodbye_1'
        await test_db.test.drop_index([('goodbye', ASCENDING)])
        assert len(await test_db.test.index_information()) == 2
        assert 'hello_1' in await test_db.test.index_information()

    @pytest.mark.asyncio
    async def test_reindex(self, test_db):
        await test_db.drop_collection('test')
        await test_db.test.insert_one({'foo': 'bar', 'who': 'what', 'when': 'how'})
        await test_db.test.create_index('foo')
        await test_db.test.create_index('who')
        await test_db.test.create_index('when')
        info = await test_db.test.index_information()

        def check_result(result):
            assert result['nIndexes'] == 4
            indexes = result['indexes']
            names = [idx['name'] for idx in indexes]
            for name in names:
                assert name in info
            for key in info:
                assert key in names

        reindexed = await test_db.test.reindex()
        if 'raw' in reindexed:
            # mongos
            for result in reindexed['raw'].values():
                check_result(result)
        else:
            check_result(reindexed)

    @pytest.mark.asyncio
    async def test_list_indexes(self, test_db):
        await test_db.test.insert_one({})  # create collection

        def map_indexes(indexes):
            return dict([(index['name'], index) for index in indexes])

        indexes = []
        async with await test_db.test.list_indexes() as cursor:
            async for ind in cursor:
                indexes.append(ind)

        assert len(indexes) == 1
        assert '_id_' in map_indexes(indexes)

        await test_db.test.create_index('hello')

        indexes = []
        async with await test_db.test.list_indexes() as cursor:
            async for ind in cursor:
                indexes.append(ind)

        assert len(indexes) == 2
        assert map_indexes(indexes)['hello_1']['key'] == SON([('hello', ASCENDING)])

        await test_db.test.create_index([('hello', DESCENDING), ('world', ASCENDING)], unique=True)

        indexes = []
        async with await test_db.test.list_indexes() as cursor:
            async for ind in cursor:
                indexes.append(ind)

        assert len(indexes) == 3
        index_map = map_indexes(indexes)
        assert index_map['hello_-1_world_1']['key'] == SON([('hello', DESCENDING), ('world', ASCENDING)])
        assert index_map['hello_-1_world_1']['unique'] == True

    @pytest.mark.asyncio
    async def test_index_info(self, test_db):
        await test_db.test.insert_one({})  # create collection
        assert len(await test_db.test.index_information()) == 1
        assert '_id_' in await test_db.test.index_information()

        await test_db.test.create_index('hello')
        assert len(await test_db.test.index_information()) == 2
        assert (await test_db.test.index_information())['hello_1']['key'] == [('hello', ASCENDING)]

        assert await test_db.test.create_index([('hello', DESCENDING), ('world', ASCENDING)], unique=True)
        assert (await test_db.test.index_information())['hello_1']['key'] == [('hello', ASCENDING)]
        assert len(await test_db.test.index_information()) == 3
        assert [('hello', DESCENDING),
                ('world', ASCENDING)] == (await test_db.test.index_information())['hello_-1_world_1']['key']
        assert (await test_db.test.index_information())['hello_-1_world_1']['unique'] is True

    @pytest.mark.asyncio
    async def test_index_geo2d(self, test_db):
        assert 'loc_2d' == await test_db.test.create_index([('loc', GEO2D)])
        index_info = (await test_db.test.index_information())['loc_2d']
        assert [('loc', '2d')] == index_info['key']

    @pytest.mark.asyncio
    async def test_index_haystack(self, mongo, test_db):

        connection = await mongo.get_connection()
        if connection.is_mongos:
            pytest.skip('Not supported via mongos')
            return

        _id = (await test_db.test.insert_one({
            'pos': {'long': 34.2, 'lat': 33.3},
            'type': 'restaurant'
        })).inserted_id
        await test_db.test.insert_one({
            'pos': {'long': 34.2, 'lat': 37.3}, 'type': 'restaurant'
        })
        await test_db.test.insert_one({
            'pos': {'long': 59.1, 'lat': 87.2}, 'type': 'office'
        })
        await test_db.test.create_index(
            [('pos', GEOHAYSTACK), ('type', ASCENDING)],
            bucketSize=1
        )

        results = (await test_db.command(SON([
            ('geoSearch', 'test'),
            ('near', [33, 33]),
            ('maxDistance', 6),
            ('search', {'type': 'restaurant'}),
            ('limit', 30),
        ])))['results']

        assert len(results) == 2
        assert {
            '_id': _id,
            'pos': {'long': 34.2, 'lat': 33.3},
            'type': 'restaurant'
        } == results[0]

    @pytest.mark.asyncio
    async def test_index_text(self, test_db):
        assert 't_text' == await test_db.test.create_index([('t', TEXT)])
        index_info = (await test_db.test.index_information())['t_text']
        assert 'weights' in index_info

        await test_db.test.insert_many([
            {'t': 'spam eggs and spam'},
            {'t': 'spam'},
            {'t': 'egg sausage and bacon'}])

        cursor = test_db.test.find(
            {'$text': {'$search': 'spam'}},
            {'score': {'$meta': 'textScore'}})

        # Sort by 'score' field.
        cursor.sort([('score', {'$meta': 'textScore'})])
        results = []

        async with cursor as cursor:
            async for res in cursor:
                results.append(res)

        assert results[0]['score'] >= results[1]['score']

    @pytest.mark.asyncio
    async def test_index_2dsphere(self, test_db):
        assert 'geo_2dsphere' == await test_db.test.create_index([('geo', GEOSPHERE)])

        for dummy, info in (await test_db.test.index_information()).items():
            field, idx_type = info['key'][0]
            if field == 'geo' and idx_type == '2dsphere':
                break
        else:
            pytest.fail('2dsphere index not found.')

        poly = {'type': 'Polygon',
                'coordinates': [[[40, 5], [40, 6], [41, 6], [41, 5], [40, 5]]]}
        query = {'geo': {'$within': {'$geometry': poly}}}

        # This query will error without a 2dsphere index.
        async with test_db.test.find(query) as cursor:
            async for _ in cursor:
                pass

    @pytest.mark.asyncio
    async def test_index_hashed(self, test_db):
        assert 'a_hashed' == await test_db.test.create_index([('a', HASHED)])

        for dummy, info in (await test_db.test.index_information()).items():
            field, idx_type = info['key'][0]
            if field == 'a' and idx_type == 'hashed':
                break
        else:
            pytest.fail('hashed index not found.')

    @pytest.mark.asyncio
    async def test_index_sparse(self, test_db):
        await test_db.test.create_index([('key', ASCENDING)], sparse=True)
        assert (await test_db.test.index_information())['key_1']['sparse']

    @pytest.mark.asyncio
    async def test_index_background(self, test_db):
        await test_db.test.create_index([('keya', ASCENDING)])
        await test_db.test.create_index([('keyb', ASCENDING)], background=False)
        await test_db.test.create_index([('keyc', ASCENDING)], background=True)
        assert 'background' not in (await test_db.test.index_information())['keya_1']
        assert not (await test_db.test.index_information())['keyb_1']['background']
        assert (await test_db.test.index_information())['keyc_1']['background']

    async def _drop_dups_setup(self, db):
        await db.drop_collection('test')
        await db.test.insert_one({'i': 1})
        await db.test.insert_one({'i': 2})
        await db.test.insert_one({'i': 2})  # duplicate
        await db.test.insert_one({'i': 3})

    @pytest.mark.asyncio
    async def test_index_dont_drop_dups(self, test_db):
        # Try *not* dropping duplicates
        await self._drop_dups_setup(test_db)

        # There's a duplicate
        with pytest.raises(DuplicateKeyError):
            await test_db.test.create_index(
                [('i', ASCENDING)],
                unique=True
            )

        # Duplicate wasn't dropped
        assert await test_db.test.count() == 4

        # Index wasn't created, only the default index on _id
        assert len(await test_db.test.index_information()) == 1

    # Get the plan dynamically because the explain format will change.
    def get_plan_stage(self, root, stage):
        if root.get('stage') == stage:
            return root
        elif 'inputStage' in root:
            return self.get_plan_stage(root['inputStage'], stage)
        elif 'inputStages' in root:
            for i in root['inputStages']:
                stage = self.get_plan_stage(i, stage)
                if stage:
                    return stage
        elif 'shards' in root:
            for i in root['shards']:
                stage = self.get_plan_stage(i['winningPlan'], stage)
                if stage:
                    return stage
        return {}

    @pytest.mark.asyncio
    async def test_index_filter(self, test_db, mongo_version):

        if not mongo_version.at_least(3, 1, 9, -1):
            return pytest.skip('Not supported on this mongo version')

        # Test bad filter spec on create.
        with pytest.raises(OperationFailure):
            await test_db.test.create_index('x', partialFilterExpression=5)
        with pytest.raises(OperationFailure):
            await test_db.test.create_index('x', partialFilterExpression={'x': {'$asdasd': 3}})
        with pytest.raises(OperationFailure):
            await test_db.test.create_index(
                'x', partialFilterExpression={
                    '$and': [{'$and': [{'x': {'$lt': 2}},
                                       {'x': {'$gt': 0}}]},
                             {'x': {'$exists': True}}]}
            )

        assert 'x_1' == await test_db.test.create_index(
            [('x', ASCENDING)], partialFilterExpression={'a': {'$lte': 1.5}}
        )
        await test_db.test.insert_one({'x': 5, 'a': 2})
        await test_db.test.insert_one({'x': 6, 'a': 1})

        # Operations that use the partial index.
        explain = await test_db.test.find({'x': 6, 'a': 1}).explain()
        stage = self.get_plan_stage(explain['queryPlanner']['winningPlan'], 'IXSCAN')
        assert 'x_1' == stage.get('indexName')
        assert stage.get('isPartial') is True

        explain = await test_db.test.find({'x': {'$gt': 1}, 'a': 1}).explain()
        stage = self.get_plan_stage(explain['queryPlanner']['winningPlan'], 'IXSCAN')
        assert 'x_1' == stage.get('indexName')
        assert stage.get('isPartial') is True

        explain = await test_db.test.find({'x': 6, 'a': {'$lte': 1}}).explain()
        stage = self.get_plan_stage(explain['queryPlanner']['winningPlan'], 'IXSCAN')
        assert 'x_1' == stage.get('indexName')
        assert stage.get('isPartial') is True

        # Operations that do not use the partial index.
        explain = await test_db.test.find({'x': 6, 'a': {'$lte': 1.6}}).explain()
        stage = self.get_plan_stage(explain['queryPlanner']['winningPlan'], 'COLLSCAN')
        assert stage != {}
        explain = await test_db.test.find({'x': 6}).explain()
        stage = self.get_plan_stage(explain['queryPlanner']['winningPlan'], 'COLLSCAN')
        assert stage != {}

        # Test drop_indexes.
        await test_db.test.drop_index('x_1')
        explain = await test_db.test.find({'x': 6, 'a': 1}).explain()
        stage = self.get_plan_stage(explain['queryPlanner']['winningPlan'], 'COLLSCAN')
        assert stage != {}

    @pytest.mark.asyncio
    async def test_field_selection(self, test_db):
        doc = {'a': 1, 'b': 5, 'c': {'d': 5, 'e': 10}}
        await test_db.test.insert_one(doc)

        # Test field inclusion
        doc = await test_db.test.find_one({}, ['_id'])
        assert list(doc) == ['_id']
        doc = await test_db.test.find_one({}, ['a'])
        l = list(doc)
        l.sort()
        assert l == ['_id', 'a']
        doc = await test_db.test.find_one({}, ['b'])
        l = list(doc)
        l.sort()
        assert l == ['_id', 'b']
        doc = await test_db.test.find_one({}, ['c'])
        l = list(doc)
        l.sort()
        assert l == ['_id', 'c']
        doc = await test_db.test.find_one({}, ['a'])
        assert doc['a'] == 1
        doc = await test_db.test.find_one({}, ['b'])
        assert doc['b'] == 5
        doc = await test_db.test.find_one({}, ['c'])
        assert doc['c'] == {'d': 5, 'e': 10}

        # Test inclusion of fields with dots
        doc = await test_db.test.find_one({}, ['c.d'])
        assert doc['c'] == {'d': 5}
        doc = await test_db.test.find_one({}, ['c.e'])
        assert doc['c'] == {'e': 10}
        doc = await test_db.test.find_one({}, ['b', 'c.e'])
        assert doc['c'] == {'e': 10}

        doc = await test_db.test.find_one({}, ['b', 'c.e'])
        l = list(doc)
        l.sort()
        assert l == ['_id', 'b', 'c']
        doc = await test_db.test.find_one({}, ['b', 'c.e'])
        assert doc['b'] == 5

        # Test field exclusion
        doc = await test_db.test.find_one({}, {'a': False, 'b': 0})
        l = list(doc)
        l.sort()
        assert l == ['_id', 'c']

        doc = await test_db.test.find_one({}, {'_id': False})
        l = list(doc)
        assert '_id' not in l

    @pytest.mark.asyncio
    async def test_options(self, test_db):
        await test_db.create_collection('test', capped=True, size=4096)
        result = await test_db.test.options()
        # mongos 2.2.x adds an $auth field when auth is enabled.
        result.pop('$auth', None)
        assert result == {'capped': True, 'size': 4096}

    @pytest.mark.asyncio
    async def test_insert_one(self, test_db):
        document = {'_id': 1000}
        result = await test_db.test.insert_one(document)
        assert isinstance(result, InsertOneResult)
        assert isinstance(result.inserted_id, int)
        assert document['_id'] == result.inserted_id
        assert result.acknowledged is True
        assert await test_db.test.find_one({'_id': document['_id']}) is not None
        assert await test_db.test.count() == 1

        document = {'foo': 'bar'}
        result = await test_db.test.insert_one(document)
        assert isinstance(result, InsertOneResult)
        assert isinstance(result.inserted_id, ObjectId)
        assert document['_id'] == result.inserted_id
        assert result.acknowledged is True
        assert await test_db.test.find_one({'_id': document['_id']}) is not None
        assert await test_db.test.count() == 2

        db = test_db.client.get_database(test_db.name, write_concern=WriteConcern(w=0))
        result = await db.test.insert_one(document)
        assert isinstance(result, InsertOneResult)
        assert isinstance(result.inserted_id, ObjectId)
        assert document['_id'], result.inserted_id
        assert result.acknowledged is False
        # The insert failed duplicate key...
        assert await db.test.count() == 2

        document = RawBSONDocument(
            BSON.encode({'_id': ObjectId(), 'foo': 'bar'})
        )
        result = await db.test.insert_one(document)
        assert isinstance(result, InsertOneResult)
        assert result.inserted_id is None

    @pytest.mark.asyncio
    async def test_insert_many(self, test_db):
        docs = [{} for _ in range(5)]
        result = await test_db.test.insert_many(docs)
        assert isinstance(result, InsertManyResult)
        assert isinstance(result.inserted_ids, list)
        assert 5 == len(result.inserted_ids)
        for doc in docs:
            _id = doc['_id']
            assert isinstance(_id, ObjectId)
            assert _id in result.inserted_ids
            assert 1 == await test_db.test.count({'_id': _id})
        assert result.acknowledged

        docs = [{'_id': i} for i in range(5)]
        result = await test_db.test.insert_many(docs)
        assert isinstance(result, InsertManyResult)
        assert isinstance(result.inserted_ids, list)
        assert 5 == len(result.inserted_ids)
        for doc in docs:
            _id = doc['_id']
            assert isinstance(_id, int)
            assert _id in result.inserted_ids
            assert 1 == await test_db.test.count({'_id': _id})
        assert result.acknowledged

        docs = [RawBSONDocument(BSON.encode({'_id': i + 5}))
                for i in range(5)]
        result = await test_db.test.insert_many(docs)
        assert isinstance(result, InsertManyResult)
        assert isinstance(result.inserted_ids, list)
        assert [] == result.inserted_ids

        db = test_db.client.get_database(test_db.name, write_concern=WriteConcern(w=0))
        docs = [{} for _ in range(5)]
        result = await db.test.insert_many(docs)
        assert isinstance(result, InsertManyResult)
        assert not result.acknowledged
        assert 20 == await db.test.count()

    @pytest.mark.asyncio
    async def test_delete_one(self, test_db):
        await test_db.test.insert_one({'x': 1})
        await test_db.test.insert_one({'y': 1})
        await test_db.test.insert_one({'z': 1})
        result = await test_db.test.delete_one({'x': 1})
        assert isinstance(result, DeleteResult)
        assert 1 == result.deleted_count
        assert result.acknowledged
        assert 2 == await test_db.test.count()

        result = await test_db.test.delete_one({'y': 1})
        assert isinstance(result, DeleteResult)
        assert 1 == result.deleted_count
        assert result.acknowledged
        assert 1 == await test_db.test.count()

        db = test_db.client.get_database(test_db.name, write_concern=WriteConcern(w=0))
        result = await db.test.delete_one({'z': 1})
        assert isinstance(result, DeleteResult)
        with pytest.raises(InvalidOperation):
            assert result.deleted_count
        assert not result.acknowledged
        assert 0 == await db.test.count()

    @pytest.mark.asyncio
    async def test_delete_many(self, test_db):
        await test_db.test.insert_one({'x': 1})
        await test_db.test.insert_one({'x': 1})
        await test_db.test.insert_one({'y': 1})
        await test_db.test.insert_one({'y': 1})

        result = await test_db.test.delete_many({'x': 1})
        assert isinstance(result, DeleteResult)
        assert 2 == result.deleted_count
        assert result.acknowledged
        assert 0 == await test_db.test.count({'x': 1})

        db = test_db.client.get_database(test_db.name, write_concern=WriteConcern(w=0))
        result = await db.test.delete_many({'y': 1})
        assert isinstance(result, DeleteResult)
        with pytest.raises(InvalidOperation):
            assert result.deleted_count
        assert not result.acknowledged
        assert 0 == await db.test.count()

    @pytest.mark.asyncio
    async def test_command_document_too_large(self, mongo, test_db):
        connection = await mongo.get_connection()
        large = '*' * (connection.max_bson_size + _COMMAND_OVERHEAD)
        coll = test_db.test
        with pytest.raises(DocumentTooLarge):
            await coll.insert_one({'data': large})
        # update_one and update_many are the same
        with pytest.raises(DocumentTooLarge):
            await coll.replace_one({}, {'data': large})

        with pytest.raises(DocumentTooLarge):
            await coll.delete_one({'data': large})

    @pytest.mark.asyncio
    async def test_insert_bypass_document_validation(self, test_db, mongo_version):

        if not mongo_version.at_least(3, 1, 9, -1):
            return pytest.skip('Not supported on this mongo version')

        await test_db.create_collection('test', validator={'a': {'$exists': True}})
        db_w0 = test_db.client.get_database(test_db.name, write_concern=WriteConcern(w=0))

        # Test insert_one
        with pytest.raises(OperationFailure):
            await test_db.test.insert_one({'_id': 1, 'x': 100})

        result = await test_db.test.insert_one(
            {'_id': 1, 'x': 100}, bypass_document_validation=True
        )
        assert isinstance(result, InsertOneResult)
        assert 1 == result.inserted_id
        result = await test_db.test.insert_one({'_id': 2, 'a': 0})
        assert isinstance(result, InsertOneResult)
        assert 2 == result.inserted_id

        with pytest.raises(OperationFailure):
            await db_w0.test.insert_one(
                {'x': 1}, bypass_document_validation=True
            )

        # Test insert_many
        docs = [{'_id': i, 'x': 100 - i} for i in range(3, 100)]
        with pytest.raises(OperationFailure):
            await test_db.test.insert_many(docs)
        result = await test_db.test.insert_many(docs, bypass_document_validation=True)
        assert isinstance(result, InsertManyResult)
        assert 97, len(result.inserted_ids)
        for doc in docs:
            _id = doc['_id']
            assert isinstance(_id, int)
            assert _id in result.inserted_ids
            assert 1 == await test_db.test.count({'x': doc['x']})
        assert result.acknowledged
        docs = [{'_id': i, 'a': 200 - i} for i in range(100, 200)]
        result = await test_db.test.insert_many(docs)
        assert isinstance(result, InsertManyResult)
        assert 97, len(result.inserted_ids)
        for doc in docs:
            _id = doc['_id']
            assert isinstance(_id, int)
            assert _id in result.inserted_ids
            assert 1 == await test_db.test.count({'a': doc['a']})
        assert result.acknowledged

        with pytest.raises(OperationFailure):
            await db_w0.test.insert_many(
                [{'x': 1}, {'x': 2}], bypass_document_validation=True
            )

    @pytest.mark.asyncio
    async def test_replace_bypass_document_validation(self, mongo, test_db, mongo_version):

        if not mongo_version.at_least(3, 1, 9, -1):
            return pytest.skip('Not supported on this mongo version')

        await test_db.create_collection('test', validator={'a': {'$exists': True}})
        db_w0 = mongo.get_database(
            test_db.name, write_concern=WriteConcern(w=0))

        # Test replace_one
        await test_db.test.insert_one({'a': 101})
        with pytest.raises(OperationFailure):
            await test_db.test.replace_one({'a': 101}, {'y': 1})

        assert 0 == await test_db.test.count({'y': 1})
        assert 1 == await test_db.test.count({'a': 101})
        await test_db.test.replace_one({'a': 101}, {'y': 1}, bypass_document_validation=True)
        assert 0 == await test_db.test.count({'a': 101})
        assert 1 == await test_db.test.count({'y': 1})
        await test_db.test.replace_one({'y': 1}, {'a': 102})
        assert 0 == await test_db.test.count({'y': 1})
        assert 0 == await test_db.test.count({'a': 101})
        assert 1 == await test_db.test.count({'a': 102})

        await test_db.test.insert_one({'y': 1}, bypass_document_validation=True)
        with pytest.raises(OperationFailure):
            await test_db.test.replace_one({'y': 1}, {'x': 101})
        assert 0 == await test_db.test.count({'x': 101})
        assert 1 == await test_db.test.count({'y': 1})
        await test_db.test.replace_one({'y': 1}, {'x': 101}, bypass_document_validation=True)
        assert 0 == await test_db.test.count({'y': 1})
        assert 1 == await test_db.test.count({'x': 101})
        await test_db.test.replace_one({'x': 101}, {'a': 103}, bypass_document_validation=False)
        assert 0 == await test_db.test.count({'x': 101})
        assert 1 == await test_db.test.count({'a': 103})

        with pytest.raises(OperationFailure):
            await db_w0.test.replace_one({'y': 1}, {'x': 1}, bypass_document_validation=True)

    @pytest.mark.asyncio
    async def test_update_bypass_document_validation(self, mongo, test_db, mongo_version):

        if not mongo_version.at_least(3, 1, 9, -1):
            return pytest.skip('Not supported on this mongo version')

        await test_db.test.insert_one({'z': 5})
        await test_db.command(SON([('collMod', 'test'), ('validator', {'z': {'$gte': 0}})]))
        db_w0 = mongo.get_database(test_db.name, write_concern=WriteConcern(w=0))

        # Test update_one
        with pytest.raises(OperationFailure):
            await test_db.test.update_one({'z': 5}, {'$inc': {'z': -10}})

        assert 0 == await test_db.test.count({'z': -5})
        assert 1 == await test_db.test.count({'z': 5})
        await test_db.test.update_one({'z': 5}, {'$inc': {'z': -10}}, bypass_document_validation=True)
        assert 0 == await test_db.test.count({'z': 5})
        assert 1 == await test_db.test.count({'z': -5})
        await test_db.test.update_one({'z': -5}, {'$inc': {'z': 6}}, bypass_document_validation=False)
        assert 1 == await test_db.test.count({'z': 1})
        assert 0 == await test_db.test.count({'z': -5})

        await test_db.test.insert_one({'z': -10}, bypass_document_validation=True)
        with pytest.raises(OperationFailure):
            await test_db.test.update_one({'z': -10}, {'$inc': {'z': 1}})
        assert 0 == await test_db.test.count({'z': -9})
        assert 1 == await test_db.test.count({'z': -10})
        await test_db.test.update_one({'z': -10}, {'$inc': {'z': 1}}, bypass_document_validation=True)
        assert 1 == await test_db.test.count({'z': -9})
        assert 0 == await test_db.test.count({'z': -10})
        await test_db.test.update_one({'z': -9}, {'$inc': {'z': 9}}, bypass_document_validation=False)
        assert 0 == await test_db.test.count({'z': -9})
        assert 1 == await test_db.test.count({'z': 0})

        with pytest.raises(OperationFailure):
            await db_w0.test.update_one({'y': 1}, {'$inc': {'x': 1}}, bypass_document_validation=True)

        # Test update_many
        await test_db.test.insert_many([{'z': i} for i in range(3, 101)])
        await test_db.test.insert_one({'y': 0}, bypass_document_validation=True)
        with pytest.raises(OperationFailure):
            await test_db.test.update_many({}, {'$inc': {'z': -100}})
        assert 100 == await test_db.test.count({'z': {'$gte': 0}})
        assert 0 == await test_db.test.count({'z': {'$lt': 0}})
        assert 0 == await test_db.test.count({'y': 0, 'z': -100})
        await test_db.test.update_many({'z': {'$gte': 0}}, {'$inc': {'z': -100}}, bypass_document_validation=True)
        assert 0 == await test_db.test.count({'z': {'$gt': 0}})
        assert 100 == await test_db.test.count({'z': {'$lte': 0}})
        await test_db.test.update_many({'z': {'$gt': -50}}, {'$inc': {'z': 100}}, bypass_document_validation=False)
        assert 50 == await test_db.test.count({'z': {'$gt': 0}})
        assert 50 == await test_db.test.count({'z': {'$lt': 0}})

        await test_db.test.insert_many([{'z': -i} for i in range(50)], bypass_document_validation=True)
        with pytest.raises(OperationFailure):
            await test_db.test.update_many({}, {'$inc': {'z': 1}})
        assert 100 == await test_db.test.count({'z': {'$lte': 0}})
        assert 50 == await test_db.test.count({'z': {'$gt': 1}})
        await test_db.test.update_many({'z': {'$gte': 0}}, {'$inc': {'z': -100}}, bypass_document_validation=True)
        assert 0 == await test_db.test.count({'z': {'$gt': 0}})
        assert 150 == await test_db.test.count({'z': {'$lte': 0}})
        await test_db.test.update_many({'z': {'$lte': 0}}, {'$inc': {'z': 100}}, bypass_document_validation=False)
        assert 150 == await test_db.test.count({'z': {'$gte': 0}})
        assert 0 == await test_db.test.count({'z': {'$lt': 0}})

        with pytest.raises(OperationFailure):
            await db_w0.test.update_many({'y': 1}, {'$inc': {'x': 1}}, bypass_document_validation=True)

    @pytest.mark.asyncio
    async def test_find_by_default_dct(self, test_db):
        await test_db.test.insert_one({'foo': 'bar'})
        dct = defaultdict(dict, [('foo', 'bar')])
        assert await test_db.test.find_one(dct) is not None
        assert dct == defaultdict(dict, [('foo', 'bar')])

    @pytest.mark.asyncio
    async def test_find_w_fields(self, test_db):
        await test_db.test.delete_many({})

        await test_db.test.insert_one(
            {'x': 1, 'mike': 'awesome', 'extra thing': 'abcdefghijklmnopqrstuvwxyz'}
        )
        assert 1 == await test_db.test.count()
        doc = await test_db.test.find_one({})
        assert 'x' in doc
        doc = await test_db.test.find_one({})
        assert 'mike' in doc
        doc = await test_db.test.find_one({})
        assert 'extra thing' in doc
        doc = await test_db.test.find_one({}, ['x', 'mike'])
        assert 'x' in doc
        doc = await test_db.test.find_one({}, ['x', 'mike'])
        assert 'mike' in doc
        doc = await test_db.test.find_one({}, ['x', 'mike'])
        assert 'extra thing' not in doc
        doc = await test_db.test.find_one({}, ['mike'])
        assert 'x' not in doc
        doc = await test_db.test.find_one({}, ['mike'])
        assert 'mike' in doc
        doc = await test_db.test.find_one({}, ['mike'])
        assert 'extra thing' not in doc

    @pytest.mark.asyncio
    async def test_fields_specifier_as_dict(self, test_db):
        await test_db.test.insert_one({'x': [1, 2, 3], 'mike': 'awesome'})

        assert [1, 2, 3] == (await test_db.test.find_one())['x']
        assert [2, 3] == (await test_db.test.find_one(projection={'x': {'$slice': -2}}))['x']
        assert 'x' not in await test_db.test.find_one(projection={'x': 0})
        assert 'mike' in await test_db.test.find_one(projection={'x': 0})

    @pytest.mark.asyncio
    async def test_find_w_regex(self, test_db):
        await test_db.test.insert_one({'x': 'hello_world'})
        await test_db.test.insert_one({'x': 'hello_mike'})
        await test_db.test.insert_one({'x': 'hello_mikey'})
        await test_db.test.insert_one({'x': 'hello_test'})

        assert await test_db.test.find().count() == 4
        assert await test_db.test.find({'x': re.compile('^hello.*')}).count() == 4
        assert await test_db.test.find({'x': re.compile('ello')}).count() == 4
        assert await test_db.test.find({'x': re.compile('^hello$')}).count() == 0
        assert await test_db.test.find({'x': re.compile('^hello_mi.*$')}).count() == 2

    @pytest.mark.asyncio
    async def test_id_can_be_anything(self, test_db):
        await test_db.test.delete_many({})
        auto_id = {'hello': 'world'}
        await test_db.test.insert_one(auto_id)
        assert isinstance(auto_id['_id'], ObjectId)

        numeric = {'_id': 240, 'hello': 'world'}
        await test_db.test.insert_one(numeric)
        assert numeric['_id'] == 240

        obj = {'_id': numeric, 'hello': 'world'}
        await test_db.test.insert_one(obj)
        assert obj['_id'] == numeric

        async with test_db.test.find() as cursor:
            async for x in cursor:
                assert x['hello'] == 'world'
                assert '_id' in x

    @pytest.mark.asyncio
    async def test_invalid_key_names(self, test_db):
        await test_db.test.insert_one({'hello': 'world'})
        await test_db.test.insert_one({'hello': {'hello': 'world'}})

        with pytest.raises(InvalidDocument):
            await test_db.test.insert_one({'$hello': 'world'})

        with pytest.raises(InvalidDocument):
            await test_db.test.insert_one({'hello': {'$hello': 'world'}})

        await test_db.test.insert_one({'he$llo': 'world'})
        await test_db.test.insert_one({'hello': {'hello$': 'world'}})

        with pytest.raises(InvalidDocument):
            await test_db.test.insert_one({'.hello': 'world'})
        with pytest.raises(InvalidDocument):
            await test_db.test.insert_one({'hello': {'.hello': 'world'}})
        with pytest.raises(InvalidDocument):
            await test_db.test.insert_one({'hello.': 'world'})
        with pytest.raises(InvalidDocument):
            await test_db.test.insert_one({'hello': {'hello.': 'world'}})
        with pytest.raises(InvalidDocument):
            await test_db.test.insert_one({'hel.lo': 'world'})
        with pytest.raises(InvalidDocument):
            await test_db.test.insert_one({'hello': {'hel.lo': 'world'}})

    @pytest.mark.asyncio
    async def test_unique_index(self, test_db):
        await test_db.test.create_index('hello')

        # No error.
        await test_db.test.insert_one({'hello': 'world'})
        await test_db.test.insert_one({'hello': 'world'})

        await test_db.drop_collection('test')
        await test_db.test.create_index('hello', unique=True)

        with pytest.raises(DuplicateKeyError):
            await test_db.test.insert_one({'hello': 'world'})
            await test_db.test.insert_one({'hello': 'world'})

    @pytest.mark.asyncio
    async def test_duplicate_key_error(self, test_db):
        await test_db.drop_collection('test')

        await test_db.test.create_index('x', unique=True)

        await test_db.test.insert_one({'_id': 1, 'x': 1})

        with pytest.raises(DuplicateKeyError) as context:
            await test_db.test.insert_one({'x': 1})

        assert context.value is not None

        with pytest.raises(DuplicateKeyError) as context:
            await test_db.test.insert_one({'x': 1})

        assert context.value is not None
        assert 1 == await test_db.test.count()

    @pytest.mark.asyncio
    async def test_write_error_text_handling(self, test_db):
        await test_db.drop_collection('test')

        await test_db.test.create_index('text', unique=True)

        # Test workaround for SERVER-24007
        data = (b'a\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83'
                b'\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83\xe2\x98\x83')

        text = data.decode('utf-8')
        await test_db.test.insert_one({'text': text})

        # Should raise DuplicateKeyError, not InvalidBSON
        with pytest.raises(DuplicateKeyError):
            await test_db.test.insert_one({'text': text})

        with pytest.raises(DuplicateKeyError):
            await test_db.test.replace_one({'_id': ObjectId()}, {'text': text}, upsert=True)

        # Should raise BulkWriteError, not InvalidBSON
        with pytest.raises(BulkWriteError):
            await test_db.test.insert_many([{'text': text}])

    @pytest.mark.asyncio
    async def test_wtimeout(self, test_db):
        # Ensure setting wtimeout doesn't disable write concern altogether.
        # See SERVER-12596.
        collection = test_db.test
        await collection.insert_one({'_id': 1})

        coll = collection.with_options(
            write_concern=WriteConcern(w=1, wtimeout=1000))
        with pytest.raises(DuplicateKeyError):
            await coll.insert_one({'_id': 1})

        coll = collection.with_options(
            write_concern=WriteConcern(wtimeout=1000))
        with pytest.raises(DuplicateKeyError):
            await coll.insert_one({'_id': 1})

    @pytest.mark.asyncio
    async def test_error_code(self, test_db):
        try:
            await test_db.test.update_many({}, {'$thismodifierdoesntexist': 1})
        except OperationFailure as exc:
            assert exc.code in (9, 10147, 16840, 17009)
            # Just check that we set the error document. Fields
            # vary by MongoDB version.
            assert exc.details is not None
        else:
            pytest.fail('OperationFailure was not raised')

    @pytest.mark.asyncio
    async def test_index_on_subfield(self, test_db):
        await test_db.drop_collection('test')

        await test_db.test.insert_one({'hello': {'a': 4, 'b': 5}})
        await test_db.test.insert_one({'hello': {'a': 7, 'b': 2}})
        await test_db.test.insert_one({'hello': {'a': 4, 'b': 10}})

        await test_db.drop_collection('test')
        await test_db.test.create_index('hello.a', unique=True)

        await test_db.test.insert_one({'hello': {'a': 4, 'b': 5}})
        await test_db.test.insert_one({'hello': {'a': 7, 'b': 2}})
        with pytest.raises(DuplicateKeyError):
            await test_db.test.insert_one({'hello': {'a': 4, 'b': 10}})

    @pytest.mark.asyncio
    async def test_replace_one(self, mongo, test_db):

        with pytest.raises(ValueError):
            await test_db.test.replace_one({}, {'$set': {'x': 1}})

        id1 = (await test_db.test.insert_one({'x': 1})).inserted_id
        result = await test_db.test.replace_one({'x': 1}, {'y': 1})
        assert isinstance(result, UpdateResult)
        assert 1 == result.matched_count
        assert result.modified_count in (None, 1)
        assert result.upserted_id is None
        assert result.acknowledged
        assert 1 == await test_db.test.count({'y': 1})
        assert 0 == await test_db.test.count({'x': 1})
        assert (await test_db.test.find_one(id1))['y'] == 1

        replacement = RawBSONDocument(BSON.encode({'_id': id1, 'z': 1}))
        result = await test_db.test.replace_one({'y': 1}, replacement, True)
        assert isinstance(result, UpdateResult)
        assert 1 == result.matched_count
        assert result.modified_count in (None, 1)
        assert result.upserted_id is None
        assert result.acknowledged
        assert 1 == await test_db.test.count({'z': 1})
        assert 0 == await test_db.test.count({'y': 1})
        assert (await test_db.test.find_one(id1))['z'] == 1

        result = await test_db.test.replace_one({'x': 2}, {'y': 2}, True)
        assert isinstance(result, UpdateResult)
        assert 0 == result.matched_count
        assert result.modified_count in (None, 0)
        assert isinstance(result.upserted_id, ObjectId)
        assert result.acknowledged
        assert 1 == await test_db.test.count({'y': 2})

        db = mongo.get_database(test_db.name, write_concern=WriteConcern(w=0))
        result = await db.test.replace_one({'x': 0}, {'y': 0})
        assert isinstance(result, UpdateResult)
        with pytest.raises(InvalidOperation):
            assert result.matched_count
        with pytest.raises(InvalidOperation):
            assert result.modified_count
        with pytest.raises(InvalidOperation):
            assert result.upserted_id
        assert not result.acknowledged

    @pytest.mark.asyncio
    async def test_update_one(self, mongo, test_db):

        with pytest.raises(ValueError):
            await test_db.test.update_one({}, {'x': 1})

        id1 = (await test_db.test.insert_one({'x': 5})).inserted_id
        result = await test_db.test.update_one({}, {'$inc': {'x': 1}})
        assert isinstance(result, UpdateResult)
        assert 1 == result.matched_count
        assert result.modified_count in (None, 1)
        assert result.upserted_id is None
        assert result.acknowledged
        assert (await test_db.test.find_one(id1))['x'] == 6

        id2 = (await test_db.test.insert_one({'x': 1})).inserted_id
        result = await test_db.test.update_one({'x': 6}, {'$inc': {'x': 1}})
        assert isinstance(result, UpdateResult)
        assert 1 == result.matched_count
        assert result.modified_count in (None, 1)
        assert result.upserted_id is None
        assert result.acknowledged
        assert (await test_db.test.find_one(id1))['x'] == 7
        assert (await test_db.test.find_one(id2))['x'] == 1

        result = await test_db.test.update_one({'x': 2}, {'$set': {'y': 1}}, True)
        assert isinstance(result, UpdateResult)
        assert 0 == result.matched_count
        assert result.modified_count in (None, 0)
        assert isinstance(result.upserted_id, ObjectId)
        assert result.acknowledged

        db = mongo.get_database(test_db.name, write_concern=WriteConcern(w=0))
        result = await db.test.update_one({'x': 0}, {'$inc': {'x': 1}})
        assert isinstance(result, UpdateResult)
        with pytest.raises(InvalidOperation):
            assert result.matched_count
        with pytest.raises(InvalidOperation):
            assert result.modified_count
        with pytest.raises(InvalidOperation):
            assert result.upserted_id
        assert not result.acknowledged

    @pytest.mark.asyncio
    async def test_update_many(self, mongo, test_db):
        await test_db.drop_collection('test')

        with pytest.raises(ValueError):
            await test_db.test.update_many({}, {'x': 1})

        await test_db.test.insert_one({'x': 4, 'y': 3})
        await test_db.test.insert_one({'x': 5, 'y': 5})
        await test_db.test.insert_one({'x': 4, 'y': 4})

        result = await test_db.test.update_many({'x': 4}, {'$set': {'y': 5}})
        assert isinstance(result, UpdateResult)
        assert 2 == result.matched_count
        assert result.modified_count in (None, 2)
        assert result.upserted_id is None
        assert result.acknowledged
        assert 3 == await test_db.test.count({'y': 5})

        result = await test_db.test.update_many({'x': 5}, {'$set': {'y': 6}})
        assert isinstance(result, UpdateResult)
        assert 1 == result.matched_count
        assert result.modified_count in (None, 1)
        assert result.upserted_id is None
        assert result.acknowledged
        assert 1 == await test_db.test.count({'y': 6})

        result = await test_db.test.update_many({'x': 2}, {'$set': {'y': 1}}, True)
        assert isinstance(result, UpdateResult)
        assert 0 == result.matched_count
        assert result.modified_count in (None, 0)
        assert isinstance(result.upserted_id, ObjectId)
        assert result.acknowledged

        db = mongo.get_database(test_db.name, write_concern=WriteConcern(w=0))
        result = await db.test.update_many({'x': 0}, {'$inc': {'x': 1}})
        assert isinstance(result, UpdateResult)
        with pytest.raises(InvalidOperation):
            assert result.matched_count
        with pytest.raises(InvalidOperation):
            assert result.modified_count
        with pytest.raises(InvalidOperation):
            assert result.upserted_id
        assert not result.acknowledged

    @pytest.mark.asyncio
    async def test_update_with_invalid_keys(self, test_db, mongo_version):
        assert await test_db.test.insert_one({'hello': 'world'})
        doc = await test_db.test.find_one()
        doc['a.b'] = 'c'

        # Replace
        with pytest.raises(OperationFailure):
            await test_db.test.replace_one({'hello': 'world'}, doc)
        # Upsert
        with pytest.raises(OperationFailure):
            await test_db.test.replace_one({'foo': 'bar'}, doc, upsert=True)

        # Check that the last two ops didn't actually modify anything
        assert 'a.b' not in await test_db.test.find_one()

        # Modify shouldn't check keys...
        assert await test_db.test.update_one(
            {'hello': 'world'}, {'$set': {'foo.bar': 'baz'}}, upsert=True
        )

        # I know this seems like testing the server but I'd like to be notified
        # by CI if the server's behavior changes here.
        doc = SON([('$set', {'foo.bar': 'bim'}), ('hello', 'world')])
        with pytest.raises(OperationFailure):
            await test_db.test.update_one(
                {'hello': 'world'}, doc, upsert=True
            )

        # This is going to cause keys to be checked and raise InvalidDocument.
        # That's OK assuming the server's behavior in the previous assert
        # doesn't change. If the behavior changes checking the first key for
        # '$' in update won't be good enough anymore.
        doc = SON([('hello', 'world'), ('$set', {'foo.bar': 'bim'})])
        with pytest.raises(OperationFailure):
            await test_db.test.replace_one({'hello': 'world'}, doc, upsert=True)

        # Replace with empty document
        assert 0 != (await test_db.test.replace_one({'hello': 'world'}, {})).matched_count

    @pytest.mark.asyncio
    async def test_acknowledged_delete(self, test_db):
        await test_db.create_collection('test', capped=True, size=1000)

        await test_db.test.insert_one({'x': 1})
        assert 1 == await test_db.test.count()

        # Can't remove from capped collection.
        with pytest.raises(OperationFailure):
            await test_db.test.delete_one({'x': 1})
        await test_db.drop_collection('test')
        await test_db.test.insert_one({'x': 1})
        await test_db.test.insert_one({'x': 1})
        assert 2 == (await test_db.test.delete_many({})).deleted_count
        assert 0 == (await test_db.test.delete_many({})).deleted_count

    @pytest.mark.asyncio
    async def test_count(self, test_db):
        await test_db.test.insert_many([{}, {}])
        assert await test_db.test.count() == 2
        await test_db.test.insert_many([{'foo': 'bar'}, {'foo': 'baz'}])
        assert await test_db.test.find({'foo': 'bar'}).count() == 1
        assert await test_db.test.count({'foo': 'bar'}) == 1
        assert await test_db.test.find({'foo': re.compile(r'ba.*')}).count() == 2
        assert await test_db.test.count({'foo': re.compile(r'ba.*')}) == 2

    @pytest.mark.asyncio
    async def test_aggregate(self, test_db):
        await test_db.test.insert_one({'foo': [1, 2]})

        with pytest.raises(TypeError):
            await test_db.test.aggregate('wow')

        pipeline = {'$project': {'_id': False, 'foo': True}}
        result = await test_db.test.aggregate([pipeline], useCursor=False)

        assert isinstance(result, CommandCursor)

        result_list = []
        async with result as cursor:
            async for item in cursor:
                result_list.append(item)
        assert [{'foo': [1, 2]}] == result_list

        out_pipeline = [pipeline, {'$out': 'output-collection'}]
        await test_db.test.aggregate(out_pipeline)

    @pytest.mark.asyncio
    async def test_aggregate_raw_bson(self, test_db):
        await test_db.test.insert_one({'foo': [1, 2]})

        with pytest.raises(TypeError):
            await test_db.test.aggregate('wow')

        pipeline = {'$project': {'_id': False, 'foo': True}}
        result = await test_db.get_collection(
            'test',
            codec_options=CodecOptions(document_class=RawBSONDocument)
        ).aggregate([pipeline], useCursor=False)
        assert isinstance(result, CommandCursor)

        first_result = None
        async for item in result:
            first_result = item
            break
        assert isinstance(first_result, RawBSONDocument)
        assert [1, 2] == list(first_result['foo'])

    @pytest.mark.asyncio
    async def test_aggregation_cursor_validation(self, test_db):
        projection = {'$project': {'_id': '$_id'}}
        cursor = await test_db.test.aggregate([projection], cursor={})
        assert isinstance(cursor, CommandCursor)

        cursor = await test_db.test.aggregate([projection], useCursor=True)
        assert isinstance(cursor, CommandCursor)

    @pytest.mark.asyncio
    async def test_aggregation_cursor(self, test_db):
        for collection_size in (10, 1000):
            await test_db.drop_collection('test')
            await test_db.test.insert_many([{'_id': i} for i in range(collection_size)])
            expected_sum = sum(range(collection_size))
            # Use batchSize to ensure multiple getMore messages
            cursor = await test_db.test.aggregate(
                [{'$project': {'_id': '$_id'}}],
                batchSize=5)

            current_sum = 0
            async with cursor:
                async for doc in cursor:
                    current_sum += doc['_id']

            assert expected_sum == current_sum

        # Test that batchSize is handled properly.
        cursor = await test_db.test.aggregate([], batchSize=5)
        assert 5 == len(cursor._CommandCursor__data)
        # Force a getMore
        cursor._CommandCursor__data.clear()

        async for _ in cursor:
            break
        # startingFrom for a command cursor doesn't include the initial batch
        # returned by the command.
        assert 5 == cursor._CommandCursor__retrieved
        # batchSize - 1
        assert 4 == len(cursor._CommandCursor__data)
        # Exhaust the cursor. There shouldn't be any errors.
        async with cursor:
            async for _ in cursor:
                pass

    @pytest.mark.asyncio
    async def test_aggregation_cursor_alive(self, test_db):
        await test_db.test.insert_many([{} for _ in range(3)])
        cursor = await test_db.test.aggregate(pipeline=[], cursor={'batchSize': 2})
        n = 0
        async for _ in cursor:
            n += 1
            if 3 == n:
                assert not cursor.alive
                break

            assert cursor.alive

    @pytest.mark.asyncio
    async def test_group(self, test_db):
        assert [] == await test_db.test.group(
            [], {}, {'count': 0}, 'function (obj, prev) { prev.count++; }'
        )

        await test_db.test.insert_many([{'a': 2}, {'b': 5}, {'a': 1}])

        assert [{'count': 3}] == await test_db.test.group(
            [], {}, {'count': 0}, 'function (obj, prev) { prev.count++; }'
        )

        assert [{'count': 1}] == await test_db.test.group(
            [], {'a': {'$gt': 1}}, {'count': 0}, 'function (obj, prev) { prev.count++; }'
        )

        await test_db.test.insert_one({'a': 2, 'b': 3})

        assert [{'a': 2, 'count': 2},
                {'a': None, 'count': 1},
                {'a': 1, 'count': 1}] == await test_db.test.group(
                    ['a'], {}, {'count': 0}, 'function (obj, prev) { prev.count++; }')

        # modifying finalize
        assert [{'a': 2, 'count': 3},
                {'a': None, 'count': 2},
                {'a': 1, 'count': 2}] == await test_db.test.group(
                    ['a'], {}, {'count': 0},
                    'function (obj, prev) { prev.count++; }',
                    'function (obj) { obj.count++; }')

        # returning finalize
        assert [2, 1, 1] == await test_db.test.group(
            ['a'], {}, {'count': 0},
            'function (obj, prev) { prev.count++; }',
            'function (obj) { return obj.count; }')

        # keyf
        assert [2, 2] == await test_db.test.group(
            'function (obj) { if (obj.a == 2) { return {a: true} }; return {b: true}; }',
            {}, {'count': 0},
            'function (obj, prev) { prev.count++; }',
            'function (obj) { return obj.count; }'
        )

        # no key
        assert [{'count': 4}] == await test_db.test.group(
            None, {}, {'count': 0}, 'function (obj, prev) { prev.count++; }'
        )

        with pytest.raises(OperationFailure):
            await test_db.test.group([], {}, {}, '5 ++ 5')

    @pytest.mark.asyncio
    async def test_large_limit(self, event_loop, test_db):
        await test_db.test_large_limit.create_index([('x', 1)])
        my_str = 'mongomongo' * 1000

        jobs = []
        for i in range(2000):
            doc = {'x': i, 'y': my_str}
            jobs.append(test_db.test_large_limit.insert_one(doc))

        done, _ = await asyncio.wait(jobs, loop=event_loop)

        assert all(ft.exception() is None for ft in done)

        i = 0
        y = 0
        async with test_db.test_large_limit.find(limit=1900).sort([('x', 1)]) as cursor:
            async for doc in cursor:
                i += 1
                y += doc['x']

        assert 1900 == i
        assert (1900 * 1899) / 2 == y

    @pytest.mark.asyncio
    async def test_find_kwargs(self, test_db):
        for i in range(10):
            await test_db.test.insert_one({'x': i})

        assert 10 == await test_db.test.count()

        total = 0
        async with test_db.test.find({}, skip=4, limit=2) as cursor:
            async for x in cursor:
                total += x['x']

        assert 9 == total

    @pytest.mark.asyncio
    async def test_rename(self, test_db):
        with pytest.raises(TypeError):
            await test_db.test.rename(5)

        with pytest.raises(InvalidName):
            await test_db.test.rename('')

        with pytest.raises(InvalidName):
            await test_db.test.rename('te$t')

        with pytest.raises(InvalidName):
            await test_db.test.rename('.test')

        with pytest.raises(InvalidName):
            await test_db.test.rename('test.')

        with pytest.raises(InvalidName):
            await test_db.test.rename('tes..t')

        assert 0 == await test_db.test.count()
        assert 0 == await test_db.foo.count()

        for i in range(10):
            await test_db.test.insert_one({'x': i})

        assert 10 == await test_db.test.count()

        await test_db.test.rename('foo')

        assert 0 == await test_db.test.count()
        assert 10 == await test_db.foo.count()

        x = 0
        async with test_db.foo.find() as cursor:
            async for doc in cursor:
                assert x == doc['x']
                x += 1

        await test_db.test.insert_one({})
        with pytest.raises(OperationFailure):
            await test_db.foo.rename('test')
        await test_db.foo.rename('test', dropTarget=True)

    @pytest.mark.asyncio
    async def test_find_one(self, test_db):

        _id = (await test_db.test.insert_one({'hello': 'world', 'foo': 'bar'})).inserted_id

        assert 'world' == (await test_db.test.find_one())['hello']
        assert await test_db.test.find_one(_id) == await test_db.test.find_one()
        assert await test_db.test.find_one(None) == await test_db.test.find_one()
        assert await test_db.test.find_one({}) == await test_db.test.find_one()
        assert await test_db.test.find_one({'hello': 'world'}) == await test_db.test.find_one()

        assert 'hello' in await test_db.test.find_one(projection=['hello'])
        assert 'hello' not in await test_db.test.find_one(projection=['foo'])
        assert ['_id'] == list(await test_db.test.find_one(projection=[]))

        assert await test_db.test.find_one({'hello': 'foo'}) is None
        assert await test_db.test.find_one(ObjectId()) is None

    @pytest.mark.asyncio
    async def test_find_one_non_objectid(self, test_db):
        await test_db.test.insert_one({'_id': 5})

        assert await test_db.test.find_one(5)
        assert not await test_db.test.find_one(6)

    @pytest.mark.asyncio
    async def test_find_one_with_find_args(self, test_db):
        await test_db.test.insert_many([{'x': i} for i in range(1, 4)])

        assert 1 == (await test_db.test.find_one())['x']
        assert 2 == (await test_db.test.find_one(skip=1))['x']

    @pytest.mark.asyncio
    async def test_find_with_sort(self, test_db):
        await test_db.test.insert_many([{'x': 2}, {'x': 1}, {'x': 3}])

        assert 2 == (await test_db.test.find_one())['x']
        assert 1 == (await test_db.test.find_one(sort=[('x', 1)]))['x']
        assert 3 == (await test_db.test.find_one(sort=[('x', -1)]))['x']

        async def to_list(things):
            res = []
            async with things as cursor:
                async for thing in cursor:
                    res.append(thing['x'])

            return res

        assert [2, 1, 3] == await to_list(test_db.test.find())
        assert [1, 2, 3] == await to_list(test_db.test.find(sort=[('x', 1)]))
        assert [3, 2, 1] == await to_list(test_db.test.find(sort=[('x', -1)]))

        with pytest.raises(TypeError):
            test_db.test.find(sort=5)
        with pytest.raises(TypeError):
            test_db.test.find(sort='hello')
        with pytest.raises(ValueError):
            test_db.test.find(sort=['hello', 1])

    @pytest.mark.asyncio
    async def test_distinct(self, test_db):
        test = test_db.test

        await test.insert_many([{'a': 1}, {'a': 2}, {'a': 2}, {'a': 2}, {'a': 3}])

        distinct = await test.distinct('a')
        distinct.sort()

        assert [1, 2, 3] == distinct

        distinct = await test.find({'a': {'$gt': 1}}).distinct('a')
        distinct.sort()
        assert [2, 3] == distinct

        distinct = await test.distinct('a', {'a': {'$gt': 1}})
        distinct.sort()
        assert [2, 3] == distinct

        await test_db.drop_collection('test')

        await test.insert_one({'a': {'b': 'a'}, 'c': 12})
        await test.insert_one({'a': {'b': 'b'}, 'c': 12})
        await test.insert_one({'a': {'b': 'c'}, 'c': 12})
        await test.insert_one({'a': {'b': 'c'}, 'c': 12})

        distinct = await test.distinct('a.b')
        distinct.sort()

        assert ['a', 'b', 'c'] == distinct

    @pytest.mark.asyncio
    async def test_query_on_query_field(self, test_db):
        await test_db.test.insert_one({'query': 'foo'})
        await test_db.test.insert_one({'bar': 'foo'})

        assert 1 == await test_db.test.find({'query': {'$ne': None}}).count()

        items = []
        async with test_db.test.find({'query': {'$ne': None}}) as cursor:
            async for item in cursor:
                items.append(item)

        assert 1 == len(items)

    @pytest.mark.asyncio
    async def test_min_query(self, test_db):
        await test_db.test.insert_many([{'x': 1}, {'x': 2}])
        await test_db.test.create_index('x')

        assert 1, len(await test_db.test.find({'$min': {'x': 2}, '$query': {}}).to_list())
        assert 2 == (await test_db.test.find({'$min': {'x': 2}, '$query': {}}).to_list())[0]['x']

    @pytest.mark.asyncio
    async def test_numerous_inserts(self, test_db):
        n_docs = 2100
        await test_db.test.insert_many([{} for _ in range(n_docs)])
        assert n_docs == await test_db.test.count()

    @pytest.mark.asyncio
    async def test_map_reduce(self, mongo, test_db):
        await test_db.test.insert_one({'id': 1, 'tags': ['dog', 'cat']})
        await test_db.test.insert_one({'id': 2, 'tags': ['cat']})
        await test_db.test.insert_one({'id': 3, 'tags': ['mouse', 'cat', 'dog']})
        await test_db.test.insert_one({'id': 4, 'tags': []})

        map = Code('function () {'
                   '  this.tags.forEach(function(z) {'
                   '    emit(z, 1);'
                   '  });'
                   '}')
        reduce = Code('function (key, values) {'
                      '  var total = 0;'
                      '  for (var i = 0; i < values.length; i++) {'
                      '    total += values[i];'
                      '  }'
                      '  return total;'
                      '}')
        result = await test_db.test.map_reduce(map, reduce, out='mrunittests')
        assert 3 == (await result.find_one({'_id': 'cat'}))['value']
        assert 2 == (await result.find_one({'_id': 'dog'}))['value']
        assert 1 == (await result.find_one({'_id': 'mouse'}))['value']

        await test_db.test.insert_one({'id': 5, 'tags': ['hampster']})
        result = await test_db.test.map_reduce(map, reduce, out='mrunittests')
        assert 1 == (await result.find_one({'_id': 'hampster'}))['value']
        await test_db.test.delete_one({'id': 5})

        result = await test_db.test.map_reduce(map, reduce, out={'merge': 'mrunittests'})
        assert 3 == (await result.find_one({'_id': 'cat'}))['value']
        assert 1 == (await result.find_one({'_id': 'hampster'}))['value']

        result = await test_db.test.map_reduce(map, reduce, out={'reduce': 'mrunittests'})

        assert 6 == (await result.find_one({'_id': 'cat'}))['value']
        assert 4 == (await result.find_one({'_id': 'dog'}))['value']
        assert 2 == (await result.find_one({'_id': 'mouse'}))['value']
        assert 1 == (await result.find_one({'_id': 'hampster'}))['value']

        result = await test_db.test.map_reduce(
            map, reduce, out={'replace': 'mrunittests'}
        )
        assert 3 == (await result.find_one({'_id': 'cat'}))['value']
        assert 2 == (await result.find_one({'_id': 'dog'}))['value']
        assert 1 == (await result.find_one({'_id': 'mouse'}))['value']

        result = await test_db.test.map_reduce(
            map, reduce, out=SON([('replace', 'mrunittests'),
                                  ('db', 'mrtestdb')])
        )
        assert 3 == (await result.find_one({'_id': 'cat'}))['value']
        assert 2 == (await result.find_one({'_id': 'dog'}))['value']
        assert 1 == (await result.find_one({'_id': 'mouse'}))['value']
        await mongo.drop_database('mrtestdb')

        full_result = await test_db.test.map_reduce(map, reduce, out='mrunittests', full_response=True)
        assert 6 == full_result['counts']['emit']

        result = await test_db.test.map_reduce(map, reduce, out='mrunittests', limit=2)
        assert 2 == (await result.find_one({'_id': 'cat'}))['value']
        assert 1 == (await result.find_one({'_id': 'dog'}))['value']
        assert await result.find_one({'_id': 'mouse'}) is None

        result = await test_db.test.map_reduce(map, reduce, out={'inline': 1})
        assert isinstance(result, dict)
        assert 'results' in result
        assert result['results'][1]['_id'] in ('cat', 'dog', 'mouse')

        result = await test_db.test.inline_map_reduce(map, reduce)
        assert isinstance(result, list)
        assert 3 == len(result)
        assert result[1]['_id'] in ('cat', 'dog', 'mouse')

        full_result = await test_db.test.inline_map_reduce(map, reduce, full_response=True)
        assert 6 == full_result['counts']['emit']

    @pytest.mark.asyncio
    async def test_messages_with_unicode_collection_names(self, test_db):
        await test_db['Employés'].insert_one({'x': 1})
        await test_db['Employés'].replace_one({'x': 1}, {'x': 2})
        await test_db['Employés'].delete_many({})
        await test_db['Employés'].find_one()
        await test_db['Employés'].find().to_list()

    @pytest.mark.asyncio
    async def test_drop_indexes_non_existent(self, test_db):
        await test_db.drop_collection('test')
        await test_db.test.drop_indexes()

    @pytest.mark.asyncio
    async def test_find_one_and(self, test_db):
        c = test_db.test
        await c.insert_one({'_id': 1, 'i': 1})

        assert {'_id': 1, 'i': 1} == await c.find_one_and_update({'_id': 1}, {'$inc': {'i': 1}})
        assert {'_id': 1, 'i': 3} == await c.find_one_and_update(
            {'_id': 1}, {'$inc': {'i': 1}}, return_document=ReturnDocument.AFTER
        )

        assert {'_id': 1, 'i': 3} == await c.find_one_and_delete({'_id': 1})
        assert await c.find_one({'_id': 1}) is None

        assert await c.find_one_and_update({'_id': 1}, {'$inc': {'i': 1}}) is None
        assert {'_id': 1, 'i': 1} == await c.find_one_and_update(
             {'_id': 1}, {'$inc': {'i': 1}},
             return_document=ReturnDocument.AFTER,
             upsert=True
        )
        assert {'_id': 1, 'i': 2} == await c.find_one_and_update(
             {'_id': 1}, {'$inc': {'i': 1}},
             return_document=ReturnDocument.AFTER
        )

        assert {'_id': 1, 'i': 3} == await c.find_one_and_replace(
             {'_id': 1}, {'i': 3, 'j': 1},
             projection=['i'],
             return_document=ReturnDocument.AFTER
        )
        assert {'i': 4} == await c.find_one_and_update(
             {'_id': 1}, {'$inc': {'i': 1}},
             projection={'i': 1, '_id': 0},
             return_document=ReturnDocument.AFTER
        )

        await c.drop()
        for j in range(5):
            await c.insert_one({'j': j, 'i': 0})

        sort = [('j', DESCENDING)]
        assert 4 == (await c.find_one_and_update({}, {'$inc': {'i': 1}}, sort=sort))['j']

    @pytest.mark.asyncio
    async def test_find_one_and_write_concern(self, test_db):
        # non-default WriteConcern.
        c_w0 = test_db.get_collection('test', write_concern=WriteConcern(w=0))
        # default WriteConcern.
        c_default = test_db.get_collection('test', write_concern=WriteConcern())

        await c_w0.find_one_and_update(
            {'_id': 1}, {'$set': {'foo': 'bar'}})

        await c_w0.find_one_and_replace({'_id': 1}, {'foo': 'bar'})

        await c_w0.find_one_and_delete({'_id': 1})

        await c_default.find_one_and_update({'_id': 1}, {'$set': {'foo': 'bar'}})

        await c_default.find_one_and_replace({'_id': 1}, {'foo': 'bar'})

        await c_default.find_one_and_delete({'_id': 1})

    @pytest.mark.asyncio
    async def test_find_with_nested(self, test_db):
        c = test_db.test
        await c.insert_many([{'i': i} for i in range(5)])  # [0, 1, 2, 3, 4]
        assert [2] == [i['i'] for i in await c.find({
            '$and': [
                {
                    # This clause gives us [1,2,4]
                    '$or': [
                        {'i': {'$lte': 2}},
                        {'i': {'$gt': 3}},
                    ],
                },
                {
                    # This clause gives us [2,3]
                    '$or': [
                        {'i': 2},
                        {'i': 3},
                    ]
                },
            ]
        }).to_list()]

        assert [0, 1, 2] == [i['i'] for i in await c.find({
            '$or': [
                {
                    # This clause gives us [2]
                    '$and': [
                        {'i': {'$gte': 2}},
                        {'i': {'$lt': 3}},
                    ],
                },
                {
                    # This clause gives us [0,1]
                    '$and': [
                        {'i': {'$gt': -100}},
                        {'i': {'$lt': 2}},
                    ]
                },
            ]
        }).to_list()]

    @pytest.mark.asyncio
    async def test_find_regex(self, test_db):
        c = test_db.test
        await c.insert_one({'r': re.compile('.*')})

        assert isinstance((await c.find_one())['r'], Regex)
        async for doc in c.find():
            assert isinstance(doc['r'], Regex)
