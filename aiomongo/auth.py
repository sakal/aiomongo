import hmac
from base64 import standard_b64decode, standard_b64encode
from hashlib import sha1
from random import SystemRandom
from typing import Callable

from bson.binary import Binary
from bson.son import SON
from pymongo.auth import (_hi, _parse_scram_response, _password_digest, _xor,
                          compare_digest, MongoCredential)
from pymongo.errors import ConfigurationError, OperationFailure

import aiomongo


async def _authenticate_scram_sha1(credentials: MongoCredential, connection: 'aiomongo.Connection') -> None:
    """Authenticate using SCRAM-SHA-1."""
    username = credentials.username
    password = credentials.password
    source = credentials.source

    # Make local
    _hmac = hmac.HMAC
    _sha1 = sha1

    user = username.encode('utf-8').replace(b'=', b'=3D').replace(b',', b'=2C')
    nonce = standard_b64encode(
        (('{}'.format(SystemRandom().random(),))[2:]).encode('utf-8'))
    first_bare = b'n=' + user + b',r=' + nonce

    cmd = SON([('saslStart', 1),
               ('mechanism', 'SCRAM-SHA-1'),
               ('payload', Binary(b'n,,' + first_bare)),
               ('autoAuthorize', 1)])
    res = await connection.command(source, cmd)

    server_first = res['payload']
    parsed = _parse_scram_response(server_first)
    iterations = int(parsed[b'i'])
    salt = parsed[b's']
    rnonce = parsed[b'r']
    if not rnonce.startswith(nonce):
        raise OperationFailure('Server returned an invalid nonce.')

    without_proof = b'c=biws,r=' + rnonce
    salted_pass = _hi(_password_digest(username, password).encode('utf-8'),
                      standard_b64decode(salt),
                      iterations)
    client_key = _hmac(salted_pass, b'Client Key', _sha1).digest()
    stored_key = _sha1(client_key).digest()
    auth_msg = b','.join((first_bare, server_first, without_proof))
    client_sig = _hmac(stored_key, auth_msg, _sha1).digest()
    client_proof = b'p=' + standard_b64encode(_xor(client_key, client_sig))
    client_final = b','.join((without_proof, client_proof))

    server_key = _hmac(salted_pass, b'Server Key', _sha1).digest()
    server_sig = standard_b64encode(
        _hmac(server_key, auth_msg, _sha1).digest())

    cmd = SON([('saslContinue', 1),
               ('conversationId', res['conversationId']),
               ('payload', Binary(client_final))])
    res = await connection.command(source, cmd)

    parsed = _parse_scram_response(res['payload'])
    if not compare_digest(parsed[b'v'], server_sig):
        raise OperationFailure('Server returned an invalid signature.')

    # Depending on how it's configured, Cyrus SASL (which the server uses)
    # requires a third empty challenge.
    if not res['done']:
        cmd = SON([('saslContinue', 1),
                   ('conversationId', res['conversationId']),
                   ('payload', Binary(b''))])
        res = await connection.command(source, cmd)
        if not res['done']:
            raise OperationFailure('SASL conversation failed to complete.')


_AUTH_MAP = {
    'SCRAM-SHA-1': _authenticate_scram_sha1,
    'DEFAULT': _authenticate_scram_sha1
}


def get_authenticator(mechanizm: str) -> Callable[[MongoCredential, 'aiomongo.Connection'], None]:
    if mechanizm not in _AUTH_MAP:
        raise ConfigurationError('Unsupported authentication type {}'.format(mechanizm))
    return _AUTH_MAP[mechanizm]
