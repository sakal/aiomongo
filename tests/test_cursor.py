import copy
import random
import re

import pytest
from bson.code import Code
from bson.son import SON

from pymongo import ASCENDING, DESCENDING, ALL, OFF
from pymongo.errors import InvalidOperation, OperationFailure


class TestCursor:

    async def _next(self, cursor):
        async for item in cursor:
            return item

        return None

    @pytest.mark.asyncio
    async def test_max_time_ms(self, test_db):
        coll = test_db.pymongo_test

        with pytest.raises(TypeError):
            coll.find().max_time_ms('foo')
        await coll.insert_one({'amalia': 1})
        await coll.insert_one({'amalia': 2})

        coll.find().max_time_ms(None)
        coll.find().max_time_ms(1)

        cursor = coll.find().max_time_ms(999)
        assert 999 == cursor._Cursor__max_time_ms
        cursor = coll.find().max_time_ms(10).max_time_ms(1000)
        assert 1000 == cursor._Cursor__max_time_ms

        cursor = coll.find().max_time_ms(999)
        assert 999 == cursor._Cursor__max_time_ms
        assert '$maxTimeMS' in cursor._Cursor__query_spec()
        assert '$maxTimeMS' in cursor._Cursor__query_spec()

        assert await coll.find_one(max_time_ms=1000)

    @pytest.mark.asyncio
    async def test_explain(self, test_db):
        a = test_db.test.find()
        await a.explain()
        async for _ in a:
            break
        b = await a.explain()
        # 'cursor' pre MongoDB 2.7.6, 'executionStats' post
        assert 'cursor' in b or 'executionStats' in b

    @pytest.mark.asyncio
    async def test_hint(self, test_db):
        with pytest.raises(TypeError):
            test_db.test.find().hint(3.5)
        await test_db.test.drop()

        await test_db.test.insert_many([{'num': i, 'foo': i} for i in range(100)])

        with pytest.raises(OperationFailure):
            await test_db.test.find({'num': 17, 'foo': 17}).hint([('num', ASCENDING)]).explain()

        with pytest.raises(OperationFailure):
            await test_db.test.find({'num': 17, 'foo': 17}).hint([('foo', ASCENDING)]).explain()

        spec = [('num', DESCENDING)]
        await test_db.test.create_index(spec)

        first = await self._next(test_db.test.find())
        assert 0 == first.get('num')
        first = await self._next(test_db.test.find().hint(spec))
        assert 99 == first.get('num')
        with pytest.raises(OperationFailure):
            await test_db.test.find({'num': 17, 'foo': 17}).hint([('foo', ASCENDING)]).explain()

        a = test_db.test.find({'num': 17})
        a.hint(spec)
        async for _ in a:
            break

        with pytest.raises(InvalidOperation):
            a.hint(spec)

    @pytest.mark.asyncio
    async def test_hint_by_name(self, test_db):
        await test_db.test.insert_many([{'i': i} for i in range(100)])

        await test_db.test.create_index([('i', DESCENDING)], name='fooindex')
        first = await self._next(test_db.test.find())
        assert 0 == first.get('i')
        first = await self._next(test_db.test.find().hint('fooindex'))
        assert 99 == first.get('i')

    @pytest.mark.asyncio
    async def test_limit(self, test_db):

        with pytest.raises(TypeError):
            test_db.test.find().limit()
        with pytest.raises(TypeError):
            test_db.test.find().limit('hello')
        with pytest.raises(TypeError):
            test_db.test.find().limit(5.5)
        assert test_db.test.find().limit(5)

        await test_db.test.drop()
        await test_db.test.insert_many([{'x': i} for i in range(100)])

        count = 0
        async for _ in test_db.test.find():
            count += 1
        assert count == 100

        count = 0
        async for _ in test_db.test.find().limit(20):
            count += 1
        assert count == 20

        count = 0
        async for _ in test_db.test.find().limit(99):
            count += 1
        assert count == 99

        count = 0
        async for _ in test_db.test.find().limit(1):
            count += 1
        assert count == 1

        count = 0
        async for _ in test_db.test.find().limit(0):
            count += 1
        assert count == 100

        count = 0
        async for _ in test_db.test.find().limit(0).limit(50).limit(10):
            count += 1
        assert count == 10

        a = test_db.test.find()
        a.limit(10)
        async for _ in a:
            break
        with pytest.raises(InvalidOperation):
            a.limit(5)

    @pytest.mark.asyncio
    async def test_max(self, test_db):
        await test_db.test.create_index([('j', ASCENDING)])

        await test_db.test.insert_many([{'j': j, 'k': j} for j in range(10)])

        cursor = test_db.test.find().max([('j', 3)])
        assert len(await cursor.to_list()) == 3

        # Tuple.
        cursor = test_db.test.find().max((('j', 3),))
        assert len(await cursor.to_list()) == 3

        # Compound index.
        await test_db.test.create_index([('j', ASCENDING), ('k', ASCENDING)])
        cursor = test_db.test.find().max([('j', 3), ('k', 3)])
        assert len(await cursor.to_list()) == 3

        # Wrong order.
        cursor = test_db.test.find().max([('k', 3), ('j', 3)])
        with pytest.raises(OperationFailure):
            await cursor.to_list()

        # No such index.
        cursor = test_db.test.find().max([('k', 3)])
        with pytest.raises(OperationFailure):
            await cursor.to_list()

        with pytest.raises(TypeError):
            test_db.test.find().max(10)

        with pytest.raises(TypeError):
            test_db.test.find().max({'j': 10})

    @pytest.mark.asyncio
    async def test_min(self, test_db):
        await test_db.test.create_index([('j', ASCENDING)])

        await test_db.test.insert_many([{'j': j, 'k': j} for j in range(10)])

        cursor = test_db.test.find().min([('j', 3)])
        assert len(await cursor.to_list()) == 7

        # Tuple.
        cursor = test_db.test.find().min((('j', 3),))
        assert len(await cursor.to_list()) == 7

        # Compound index.
        await test_db.test.create_index([('j', ASCENDING), ('k', ASCENDING)])
        cursor = test_db.test.find().min([('j', 3), ('k', 3)])
        assert len(await cursor.to_list()) == 7

        # Wrong order.
        cursor = test_db.test.find().min([('k', 3), ('j', 3)])
        with pytest.raises(OperationFailure):
            await cursor.to_list()

        # No such index.
        cursor = test_db.test.find().min([('k', 3)])
        with pytest.raises(OperationFailure):
            await cursor.to_list()

        with pytest.raises(TypeError):
            test_db.test.find().min(10)

        with pytest.raises(TypeError):
            test_db.test.find().min({'j': 10})

    @pytest.mark.asyncio
    async def test_batch_size(self, test_db, mongo_version):
        await test_db.test.insert_many([{'x': x} for x in range(200)])

        with pytest.raises(TypeError):
            test_db.test.find().batch_size(None)
        with pytest.raises(TypeError):
            test_db.test.find().batch_size('hello')
        with pytest.raises(TypeError):
            test_db.test.find().batch_size(5.5)
        with pytest.raises(ValueError):
            test_db.test.find().batch_size(-1)
        assert test_db.test.find().batch_size(5)
        a = test_db.test.find()
        async for _ in a:
            break

        with pytest.raises(InvalidOperation):
            a.batch_size(5)

        async def cursor_count(cursor, expected_count):
            count = 0
            async with cursor:
                async for _ in cursor:
                    count += 1
            assert expected_count, count

        await cursor_count(test_db.test.find().batch_size(0), 200)
        await cursor_count(test_db.test.find().batch_size(1), 200)
        await cursor_count(test_db.test.find().batch_size(2), 200)
        await cursor_count(test_db.test.find().batch_size(5), 200)
        await cursor_count(test_db.test.find().batch_size(100), 200)
        await cursor_count(test_db.test.find().batch_size(500), 200)

        await cursor_count(test_db.test.find().batch_size(0).limit(1), 1)
        await cursor_count(test_db.test.find().batch_size(1).limit(1), 1)
        await cursor_count(test_db.test.find().batch_size(2).limit(1), 1)
        await cursor_count(test_db.test.find().batch_size(5).limit(1), 1)
        await cursor_count(test_db.test.find().batch_size(100).limit(1), 1)
        await cursor_count(test_db.test.find().batch_size(500).limit(1), 1)

        await cursor_count(test_db.test.find().batch_size(0).limit(10), 10)
        await cursor_count(test_db.test.find().batch_size(1).limit(10), 10)
        await cursor_count(test_db.test.find().batch_size(2).limit(10), 10)
        await cursor_count(test_db.test.find().batch_size(5).limit(10), 10)
        await cursor_count(test_db.test.find().batch_size(100).limit(10), 10)
        await cursor_count(test_db.test.find().batch_size(500).limit(10), 10)

        cur = test_db.test.find().batch_size(1)
        await self._next(cur)
        if mongo_version.at_least(3, 1, 9):
            # find command batchSize should be 1
            assert 0 == len(cur._Cursor__data)
        else:
            # OP_QUERY ntoreturn should be 2
            assert 1 == len(cur._Cursor__data)
        await self._next(cur)
        assert 0 == len(cur._Cursor__data)
        await self._next(cur)
        assert 0 == len(cur._Cursor__data)
        await self._next(cur)
        assert 0 == len(cur._Cursor__data)

    @pytest.mark.asyncio
    async def test_limit_and_batch_size(self, test_db):
        await test_db.test.insert_many([{'x': x} for x in range(500)])

        curs = test_db.test.find().limit(0).batch_size(10)
        await self._next(curs)
        assert 10 == curs._Cursor__retrieved

        curs = test_db.test.find(limit=0, batch_size=10)
        await self._next(curs)
        assert 10 == curs._Cursor__retrieved

        curs = test_db.test.find().limit(-2).batch_size(0)
        await self._next(curs)
        assert 2 == curs._Cursor__retrieved

        curs = test_db.test.find(limit=-2, batch_size=0)
        await self._next(curs)
        assert 2 == curs._Cursor__retrieved

        curs = test_db.test.find().limit(-4).batch_size(5)
        await self._next(curs)
        assert 4 == curs._Cursor__retrieved

        curs = test_db.test.find(limit=-4, batch_size=5)
        await self._next(curs)
        assert 4 == curs._Cursor__retrieved

        curs = test_db.test.find().limit(50).batch_size(500)
        await self._next(curs)
        assert 50 == curs._Cursor__retrieved

        curs = test_db.test.find(limit=50, batch_size=500)
        await self._next(curs)
        assert 50 == curs._Cursor__retrieved

        curs = test_db.test.find().batch_size(500)
        await self._next(curs)
        assert 500 == curs._Cursor__retrieved

        curs = test_db.test.find(batch_size=500)
        await self._next(curs)
        assert 500 == curs._Cursor__retrieved

        curs = test_db.test.find().limit(50)
        await self._next(curs)
        assert 50 == curs._Cursor__retrieved

        curs = test_db.test.find(limit=50)
        await self._next(curs)
        assert 50 == curs._Cursor__retrieved

        # these two might be shaky, as the default
        # is set by the server. as of 3.2.0, 101
        # or 1MB (whichever is smaller) is default
        # for queries without ntoreturn
        curs = test_db.test.find()
        await self._next(curs)
        # TODO: Investigate why 100 not 101?
        assert 100 == curs._Cursor__retrieved

        curs = test_db.test.find().limit(0).batch_size(0)
        await self._next(curs)
        assert 101 == curs._Cursor__retrieved

        curs = test_db.test.find(limit=0, batch_size=0)
        await self._next(curs)
        assert 101 == curs._Cursor__retrieved

    @pytest.mark.asyncio
    async def test_skip(self, test_db):
        with pytest.raises(TypeError):
            test_db.test.find().skip(None)
        with pytest.raises(TypeError):
            test_db.test.find().skip('hello')
        with pytest.raises(TypeError):
            test_db.test.find().skip(5.5)
        with pytest.raises(ValueError):
            test_db.test.find().skip(-5)
        assert test_db.test.find().skip(5)

        await test_db.test.insert_many([{'x': i} for i in range(100)])

        async for i in test_db.test.find():
            assert i['x'] == 0
            break

        async for i in test_db.test.find().skip(20):
            assert i['x'] == 20
            break

        async for i in test_db.test.find().skip(99):
            assert i['x'] == 99
            break

        async for i in test_db.test.find().skip(1):
            assert i['x'] == 1
            break

        async for i in test_db.test.find().skip(0):
            assert i['x'] == 0
            break

        async for i in test_db.test.find().skip(0).skip(50).skip(10):
            assert i['x'] == 10
            break

        async for _ in test_db.test.find().skip(1000):
            pytest.fail('Shouldn\'t receive any documents')

        a = test_db.test.find()
        a.skip(10)
        async for _ in a:
            break
        with pytest.raises(InvalidOperation):
            a.skip(5)

    @pytest.mark.asyncio
    async def test_sort(self, test_db):

        with pytest.raises(TypeError):
            test_db.test.find().sort(5)

        with pytest.raises(ValueError):
            test_db.test.find().sort([])
        with pytest.raises(TypeError):
            test_db.test.find().sort([], ASCENDING)
        with pytest.raises(TypeError):
            test_db.test.find().sort([], [('hello', DESCENDING)], DESCENDING)

        unsort = list(range(10))
        random.shuffle(unsort)

        await test_db.test.insert_many([{'x': i} for i in unsort])

        asc = [i['x'] for i in await test_db.test.find().sort('x', ASCENDING).to_list()]
        assert asc == list(range(10))
        asc = [i['x'] for i in await test_db.test.find().sort('x').to_list()]
        assert asc == list(range(10))
        asc = [i['x'] for i in await test_db.test.find().sort([('x', ASCENDING)]).to_list()]
        assert asc == list(range(10))

        expect = list(reversed(range(10)))
        desc = [i['x'] for i in await test_db.test.find().sort('x', DESCENDING).to_list()]
        assert desc == expect
        desc = [i['x'] for i in await test_db.test.find().sort([('x', DESCENDING)]).to_list()]
        assert desc == expect
        desc = [i['x'] for i in
                await test_db.test.find().sort('x', ASCENDING).sort('x', DESCENDING).to_list()]
        assert desc == expect

        expected = [(1, 5), (2, 5), (0, 3), (7, 3), (9, 2), (2, 1), (3, 1)]
        shuffled = list(expected)
        random.shuffle(shuffled)

        await test_db.test.drop()
        for (a, b) in shuffled:
            await test_db.test.insert_one({'a': a, 'b': b})

        result = [(i['a'], i['b']) for i in
                  await test_db.test.find().sort([('b', DESCENDING),
                                                 ('a', ASCENDING)]).to_list()]
        assert result == expected

        a = test_db.test.find()
        a.sort('x', ASCENDING)
        async for _ in a:
            break

        with pytest.raises(InvalidOperation):
            a.sort('x', ASCENDING)

    @pytest.mark.asyncio
    async def test_count(self, test_db):

        assert 0 == await test_db.test.find().count()

        await test_db.test.insert_many([{'x': i} for i in range(10)])

        assert 10 == await test_db.test.find().count()
        assert isinstance(await test_db.test.find().count(), int)
        assert 10 == await test_db.test.find().limit(5).count()
        assert 10 == await test_db.test.find().skip(5).count()

        assert 1 == await test_db.test.find({'x': 1}).count()
        assert 5 == await test_db.test.find({'x': {'$lt': 5}}).count()

        a = test_db.test.find()
        b = await a.count()
        async for _ in a:
            break
        assert b == await a.count()

        assert 0 == await test_db.test.acollectionthatdoesntexist.find().count()

    @pytest.mark.asyncio
    async def test_count_with_hint(self, test_db, mongo_version):
        collection = test_db.test

        await collection.insert_many([{'i': 1}, {'i': 2}])
        assert 2 == await collection.find().count()

        await collection.create_index([('i', 1)])

        assert 1 == await collection.find({'i': 1}).hint('_id_').count()
        assert 2 == await collection.find().hint('_id_').count()

        with pytest.raises(OperationFailure):
            await collection.find({'i': 1}).hint('BAD HINT').count()

        # Create a sparse index which should have no entries.
        await collection.create_index([('x', 1)], sparse=True)

        assert 0 == await collection.find({'i': 1}).hint('x_1').count()
        assert 0 == await collection.find({'i': 1}).hint([('x', 1)]).count()

        if mongo_version.at_least(3, 3, 2):
            assert 0 == await collection.find().hint('x_1').count()
            assert 0 == await collection.find().hint([('x' == 1)]).count()
        else:
            assert 2 == await collection.find().hint('x_1').count()
            assert 2 == await collection.find().hint([('x', 1)]).count()

    @pytest.mark.asyncio
    async def test_where(self, test_db):
        a = test_db.test.find()

        with pytest.raises(TypeError):
            a.where(5)
        with pytest.raises(TypeError):
            a.where(None)
        with pytest.raises(TypeError):
            a.where({})

        await test_db.test.insert_many([{'x': i} for i in range(10)])

        assert 3 == len(await test_db.test.find().where('this.x < 3').to_list())
        assert 3 == len(await test_db.test.find().where(Code('this.x < 3')).to_list())
        assert 3 == len(await test_db.test.find().where(Code('this.x < i', {'i': 3})).to_list())
        assert 10 == len(await test_db.test.find().to_list())

        assert 3 == await test_db.test.find().where('this.x < 3').count()
        assert 10 == await test_db.test.find().count()
        assert 3 == await test_db.test.find().where(u'this.x < 3').count()
        assert [0, 1, 2] == [a['x'] for a in await test_db.test.find().where('this.x < 3').to_list()]
        assert [] == [a['x'] for a in await test_db.test.find({'x': 5}).where('this.x < 3').to_list()]
        assert [5] == [a['x'] for a in await test_db.test.find({'x': 5}).where('this.x > 3').to_list()]

        cursor = test_db.test.find().where('this.x < 3').where('this.x > 7')
        assert [8, 9] == [a['x'] for a in await cursor.to_list()]

        a = test_db.test.find()
        b = a.where('this.x > 3')
        async for _ in a:
            break
        with pytest.raises(InvalidOperation):
            a.where('this.x < 3')

    @pytest.mark.asyncio
    async def test_rewind(self, test_db):
        await test_db.test.insert_many([{'x': i} for i in range(1, 4)])

        cursor = test_db.test.find().limit(2)

        count = 0
        async for _ in cursor:
            count += 1
        assert 2 == count

        count = 0
        async for _ in cursor:
            count += 1
        assert 0 == count

        cursor.rewind()
        count = 0
        async for _ in cursor:
            count += 1
        assert 2 == count

        cursor.rewind()
        count = 0
        async for _ in cursor:
            break
        cursor.rewind()
        async for _ in cursor:
            count += 1
        assert 2 == count

        assert cursor == cursor.rewind()

    @pytest.mark.asyncio
    async def test_clone(self, test_db):
        await test_db.test.insert_many([{'x': i} for i in range(1, 4)])

        cursor = test_db.test.find().limit(2)

        count = 0
        async for _ in cursor:
            count += 1
        assert 2 == count

        count = 0
        async for _ in cursor:
            count += 1
        assert 0 == count

        cursor = cursor.clone()
        cursor2 = cursor.clone()
        count = 0
        async for _ in cursor:
            count += 1
        assert 2 == count
        async for _ in cursor2:
            count += 1
        assert 4 == count

        cursor.rewind()
        count = 0
        async for _ in cursor:
            break
        cursor = cursor.clone()
        async for _ in cursor:
            count += 1
        assert 2 == count

        assert cursor != cursor.clone()

        # Just test attributes
        cursor = test_db.test.find({'x': re.compile('^hello.*')},
                                   skip=1,
                                   no_cursor_timeout=True,
                                   projection={'_id': False}).limit(2)
        cursor.min([('a', 1)]).max([('b', 3)])
        cursor.add_option(128)
        cursor.comment('hi!')

        cursor2 = cursor.clone()
        assert cursor._Cursor__skip == cursor2._Cursor__skip
        assert cursor._Cursor__limit == cursor2._Cursor__limit
        assert type(cursor._Cursor__codec_options) == type(cursor2._Cursor__codec_options)
        assert cursor._Cursor__query_flags == cursor2._Cursor__query_flags
        assert cursor._Cursor__comment == cursor2._Cursor__comment
        assert cursor._Cursor__min == cursor2._Cursor__min
        assert cursor._Cursor__max == cursor2._Cursor__max

        # Shallow copies can so can mutate
        cursor2 = copy.copy(cursor)
        cursor2._Cursor__projection['cursor2'] = False
        assert 'cursor2' in cursor._Cursor__projection

        # Deepcopies and shouldn't mutate
        cursor3 = copy.deepcopy(cursor)
        cursor3._Cursor__projection['cursor3'] = False
        assert 'cursor3' not in cursor._Cursor__projection

        cursor4 = cursor.clone()
        cursor4._Cursor__projection['cursor4'] = False
        assert 'cursor4' not in cursor._Cursor__projection

        # Test memo when deepcopying queries
        query = {'hello': 'world'}
        query['reflexive'] = query
        cursor = test_db.test.find(query)

        cursor2 = copy.deepcopy(cursor)

        assert id(cursor._Cursor__spec) != id(cursor2._Cursor__spec)
        assert id(cursor2._Cursor__spec['reflexive']) == id(cursor2._Cursor__spec)
        assert len(cursor2._Cursor__spec) == 2

        # Ensure hints are cloned as the correct type
        cursor = test_db.test.find().hint([('z', 1), ('a', 1)])
        cursor2 = copy.deepcopy(cursor)
        assert isinstance(cursor2._Cursor__hint, SON)
        assert cursor._Cursor__hint == cursor2._Cursor__hint

    @pytest.mark.asyncio
    async def test_count_with_fields(self, test_db):
        await test_db.test.insert_one({'x': 1})
        assert 1 == await test_db.test.find({}, ['a']).count()

    @pytest.mark.asyncio
    async def test_count_with_limit_and_skip(self, test_db):
        with pytest.raises(TypeError):
            await test_db.test.find().count('foo')

        async def check_len(cursor, length):
            assert len(await cursor.to_list()) == await cursor.count(True)
            assert length == await cursor.count(True)

        await test_db.test.insert_many([{'i': i} for i in range(100)])

        await check_len(test_db.test.find(), 100)

        await check_len(test_db.test.find().limit(10), 10)
        await check_len(test_db.test.find().limit(110), 100)

        await check_len(test_db.test.find().skip(10), 90)
        await check_len(test_db.test.find().skip(110), 0)

        await check_len(test_db.test.find().limit(10).skip(10), 10)
        await check_len(test_db.test.find().limit(10).skip(95), 5)

    @pytest.mark.asyncio
    async def test_len(self, test_db):
        with pytest.raises(TypeError):
            len(test_db.test.find())

    @pytest.mark.asyncio
    async def test_get_more(self, test_db):
        await test_db.test.insert_many([{'i': i} for i in range(10)])
        count = 0
        async with test_db.test.find().batch_size(5) as cursor:
            async for _ in cursor:
                count += 1
        assert 10 == count

    @pytest.mark.asyncio
    async def test_distinct(self, test_db):

        await test_db.test.insert_many(
            [{'a': 1}, {'a': 2}, {'a': 2}, {'a': 2}, {'a': 3}])

        distinct = await test_db.test.find({'a': {'$lt': 3}}).distinct('a')
        distinct.sort()

        assert [1, 2] == distinct

        await test_db.drop_collection('test')

        await test_db.test.insert_one({'a': {'b': 'a'}, 'c': 12})
        await test_db.test.insert_one({'a': {'b': 'b'}, 'c': 8})
        await test_db.test.insert_one({'a': {'b': 'c'}, 'c': 12})
        await test_db.test.insert_one({'a': {'b': 'c'}, 'c': 8})

        distinct = await test_db.test.find({'c': 8}).distinct('a.b')
        distinct.sort()

        assert ['b', 'c'] == distinct

    @pytest.mark.asyncio
    async def test_max_scan(self, test_db):
        await test_db.test.insert_many([{} for _ in range(100)])

        assert 100 == len(await test_db.test.find().to_list())
        assert 50 == len(await test_db.test.find().max_scan(50).to_list())
        assert 50 == len(await test_db.test.find().max_scan(90).max_scan(50).to_list())

    @pytest.mark.asyncio
    async def test_comment(self, mongo, test_db, mongo_version):
        connection = await mongo.get_connection()
        if connection.is_mongos:
            pytest.skip('Not supported via mongos')
            return

        # MongoDB 3.1.5 changed the ns for commands.
        regex = {'$regex': '{}.(\$cmd|test)'.format(test_db.name)}

        if mongo_version.at_least(3, 1, 8, -1):
            query_key = 'query.comment'
        else:
            query_key = 'query.$comment'

        await test_db.set_profiling_level(ALL)
        try:
            list(await test_db.test.find().comment('foo').to_list())
            op = test_db.system.profile.find({
                'ns': '{}.test'.format(test_db.name),
                'op': 'query',
                query_key: 'foo'
            })
            assert await op.count() == 1

            await test_db.test.find().comment('foo').count()
            op = test_db.system.profile.find({
                'ns': regex,
                'op': 'command',
                'command.count': 'test',
                'command.$comment': 'foo'
            })
            assert await op.count() == 1

            await test_db.test.find().comment('foo').distinct('type')
            op = test_db.system.profile.find({
                'ns': regex,
                'op': 'command',
                'command.distinct': 'test',
                'command.$comment': 'foo'
            })
            assert await op.count() == 1
        finally:
            await test_db.set_profiling_level(OFF)
            await test_db.system.profile.drop()

        await test_db.test.insert_many([{}, {}])
        cursor = test_db.test.find()
        await self._next(cursor)
        with pytest.raises(InvalidOperation):
            cursor.comment('hello')

    @pytest.mark.asyncio
    async def test_modifiers(self, test_db):
        cur = test_db.test.find()
        assert '$query' not in cur._Cursor__query_spec()
        cur = test_db.test.find().comment('testing').max_time_ms(500)
        assert '$query' in cur._Cursor__query_spec()
        assert cur._Cursor__query_spec()['$comment'] == 'testing'
        assert cur._Cursor__query_spec()['$maxTimeMS'] == 500
        cur = test_db.test.find(
            modifiers={'$maxTimeMS': 500, '$comment': 'testing'})
        assert '$query' in cur._Cursor__query_spec()
        assert cur._Cursor__query_spec()['$comment'] == 'testing'
        assert cur._Cursor__query_spec()['$maxTimeMS'] == 500

    @pytest.mark.asyncio
    async def test_alive(self, test_db):
        await test_db.test.insert_many([{} for _ in range(3)])
        cursor = test_db.test.find().batch_size(2)
        n = 0
        while True:
            await self._next(cursor)
            n += 1
            if 3 == n:
                assert not cursor.alive
                break

            assert cursor.alive
