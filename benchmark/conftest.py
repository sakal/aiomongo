import asyncio
import os

import pymongo
import pytest

import aiomongo
from motor.motor_asyncio import AsyncIOMotorClient


HOST = os.getenv('MONGO_HOST', 'localhost')
PORT = int(os.getenv('MONGO_PORT', 27017))
CONNECTION_STRING = 'mongodb://{}:{}/aiomongo_test?maxpoolsize=10'.format(HOST, PORT)


@pytest.fixture(scope='session', autouse=True)
def dropdb(aio_loop):
    cl = AsyncIOMotorClient(CONNECTION_STRING)
    db = cl.get_default_database()
    aio_loop.run_until_complete(cl.drop_database(db))
    aio_loop.run_until_complete(db.test.create_index([('a', pymongo.ASCENDING)]))


@pytest.fixture(scope='session')
def aio_loop():
    return asyncio.get_event_loop()


@pytest.fixture(scope='session')
def aio_mongo_db(aio_loop):
    cl = aio_loop.run_until_complete(
        aiomongo.create_client(CONNECTION_STRING, aio_loop)
    )
    db = cl.get_default_database()
    yield db
    cl.close()
    aio_loop.run_until_complete(cl.wait_closed())


@pytest.fixture(scope='session')
def motor_mongo_db():
    cl = AsyncIOMotorClient(CONNECTION_STRING)
    db = cl.get_default_database()
    yield db
    cl.close()
