#!/usr/bin/env python
from setuptools import setup

setup(
    name='aiomongo',
    version='0.1',
    description='Asynchronous Python driver for MongoDB <http://www.mongodb.org>',
    author='Dmytro Domashevskyi',
    author_email='domash@zeoalliance.com',
    url='https://github.com/ZeoAlliance/aiomongo',
    keywords=['mongo', 'mongodb', 'pymongo', 'aiomongo'],
    packages=['aiomongo'],
    install_requires=['pymongo>=3.3'],
    license='Apache License, Version 2.0',
    include_package_data=True,
    tests_require=['pytest==3.0.3', 'pytest-asyncio==0.5.0', 'coverage'],
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