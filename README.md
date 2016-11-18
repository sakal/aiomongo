# Asynchronous MongoDB client based on asyncio.

Aiomongo is a rewrite of [Pymongo](https://github.com/mongodb/mongo-python-driver) driver that uses [asyncio](https://docs.python.org/3/library/asyncio.html) for networking. The reason why
this client was written is that an alternative [Motor](https://github.com/mongodb/motor) implementation uses thread pool
to avoid blocking on network operations. But it appears (see benchmarks below) to work slower than pure asyncio implementation.

# Status

It is not yet full featured implementation and not yet battle tested, though loads of integration tests were ported from pymongo and
aiomongo successfully passes it.
Missing features are:

- [ ] GridFS
- [ ] Bulk write API 
- [ ] Replica set: you can only connect to single host - either single mongod instance or mongos (in front of replica set).
- [ ] Authentication: The only method that is supported is SCRAM-SHA-1 (default for MongoDB as of version 3.2)

# Dependencies

- Unix, including Mac OS X. Wasn't tested on Windows.
- PyMongo 3.3 or later
- Python 3.5.2 or later
- MongoDB 3.0 or later

# Features

The client aims to have as close API as the official PyMongo driver while using the features of asyncio. It also uses type hints in it's API
to make most of your linters.

Some samples below.


### Connecting to database:

Client uses maxpoolsize parameter by making this amount of database connections at startup.

```python
import asyncio

import aiomongo

async def main(loop):
    client = await aiomongo.create_client('mongodb://localhost:27017/test?w=2&maxpoolsize=10', loop=loop)
    db = client.get_default_database()
    
    await db.items.insert_one({'x': 1})
    await db.items.delete_one({'x': 1})
    
    client.close()
    await client.wait_closed()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
```


### Using cursor

The following code will iterate over find result loading int memory only 10 items at a time.
```python

async with db.items.find().batch_size(10).limit(100) as cursor:
    async for item in cursor:
        print(item)
```

If you need to load the whole result into memory with one call, you could use:
```python
items = await db.items.find().batch_size(10).limit(100).to_list()
```

# Testing

Aiomongo uses py.test as testing framework. If you need to run tests fast you can use Docker:

```
docker-compose run tests
```

Or set install necessary requirements from dev-requirements.txt and run:
```
py.test ./tests
```

# Benchmarks

There is a small benchmark suite that you can run yourself. It runs different numbers of coroutines doing queries at the same time.
On our machines it is up to 3 times faster than Motor, on big number of concurrent queries.

Again you can quickly run benchmarks inside docker container like
```
docker-compose run benchmarks
```

Or install dependencies from dev-requirements.txt yourself and from the project directory run:
```
py.test ./benchmark --benchmark-json=benchmark.json --benchmark-warmup=on --benchmark-warmup-iterations=10 && python ./benchmark/plotbench.py
```

### Sample results (lower is better):

#### find_one results

![find_one results](/benchmark/test_find_one.png?raw=true "find_one")

#### Iterating cursor with find
![find results](/benchmark/test_find.png?raw=true "find")

#### insert_one results
![insert_one results](/benchmark/test_insert_one.png?raw=true "find")
