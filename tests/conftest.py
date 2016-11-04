import os

import pytest

import aiomongo
from .version import Version


HOST = os.getenv('DB_IP', 'localhost')
PORT = int(os.getenv('DB_PORT', 27017))


@pytest.fixture(scope='function')
def mongo(event_loop):
    conn_string = 'mongodb://{}:{}/aiomongo_test?maxpoolsize=1'.format(HOST, PORT)
    client = event_loop.run_until_complete(
        aiomongo.create_client(conn_string, event_loop)
    )
    yield client
    client.close()
    event_loop.run_until_complete(client.wait_closed())


@pytest.fixture(scope='function')
def test_db(event_loop, mongo):
    db = mongo.get_default_database()
    event_loop.run_until_complete(mongo.drop_database(db))
    return db


@pytest.fixture(scope='function')
def mongo_version(event_loop, mongo):
    server_info = event_loop.run_until_complete(
        mongo.server_info()
    )
    if 'versionArray' in server_info:
        return Version.from_version_array(server_info['versionArray'])
    return Version.from_string(server_info['version'])


