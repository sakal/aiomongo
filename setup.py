import sys
from setuptools import setup

if sys.version_info < (3, 5, 2):
    raise RuntimeError('aiomongo requires Python 3.5.2+')


install_requires = ['pymongo>=3.3']
tests_require = install_requires + ['pytest', 'pytest-asyncio'],

setup(
    name='aiomongo',
    version='0.1',
    description='Asynchronous Python driver for MongoDB <http://www.mongodb.org>',
    author='Dmytro Domashevskyi',
    author_email='domash@zeoalliance.com',
    url='https://github.com/ZeoAlliance/aiomongo',
    keywords=['mongo', 'mongodb', 'pymongo', 'aiomongo'],
    packages=['aiomongo'],
    install_requires=install_requires,
    license='Apache License, Version 2.0',
    include_package_data=True,
    tests_require=tests_require,
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Database']
    )