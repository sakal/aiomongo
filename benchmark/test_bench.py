import asyncio

import pytest


concurrencies = [1, 2, 4, 8, 16, 32, 64, 96, 128, 160, 192, 256]


class TestBenchmark:

    @staticmethod
    def _insert_one_function(aio_loop, db, concurrency):
        aio_loop.run_until_complete(
            asyncio.wait([db.test.insert_one({'a': i}) for i in range(concurrency)], loop=aio_loop)
        )

    @staticmethod
    def _find_one_function(aio_loop, db, concurrency):
        aio_loop.run_until_complete(
            asyncio.wait([db.test.find_one({'a': i}) for i in range(concurrency)], loop=aio_loop)
        )

    @staticmethod
    async def _exhaust_cursor(cursor):
        async for _ in cursor:
            pass

    def _find_function(self, aio_loop, db, concurrency):
        aio_loop.run_until_complete(
            asyncio.wait([self._exhaust_cursor(db.test.find({}).limit(200)) for _ in range(concurrency)], loop=aio_loop)
        )

    @pytest.mark.parametrize('concurrency', concurrencies)
    def test_insert_one_aiomongo(self, aio_loop, aio_mongo_db, benchmark, concurrency):
        benchmark(self._insert_one_function, aio_loop, aio_mongo_db, concurrency)

    @pytest.mark.parametrize('concurrency', concurrencies)
    def test_insert_one_motor(self, aio_loop, motor_mongo_db, benchmark, concurrency):
        benchmark(self._insert_one_function, aio_loop, motor_mongo_db, concurrency)

    @pytest.mark.parametrize('concurrency', concurrencies)
    def test_find_one_aiomongo(self, aio_loop, aio_mongo_db, benchmark, concurrency):
        benchmark(self._find_one_function, aio_loop, aio_mongo_db, concurrency)

    @pytest.mark.parametrize('concurrency', concurrencies)
    def test_find_one_motor(self, aio_loop, motor_mongo_db, benchmark, concurrency):
        benchmark(self._find_one_function, aio_loop, motor_mongo_db, concurrency)

    @pytest.mark.parametrize('concurrency', concurrencies)
    def test_find_aiomongo(self, aio_loop, aio_mongo_db, benchmark, concurrency):
        benchmark(self._find_function, aio_loop, aio_mongo_db, concurrency)

    @pytest.mark.parametrize('concurrency', concurrencies)
    def test_find_motor(self, aio_loop, motor_mongo_db, benchmark, concurrency):
        benchmark(self._find_function, aio_loop, motor_mongo_db, concurrency)
