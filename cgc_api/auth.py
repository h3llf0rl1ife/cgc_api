from flask import request
from flask_restful import Resource

from cgc_api.config import CURRENT_CONFIG, SECRET
from cgc_api.query import Query
from cgc_api.crypto import Crypto


class AccessAuth(Resource):
    def get(self):
        pass

    def post(self):
        auth = request.data

        if auth:
            crypto = Crypto(SECRET)
            hash_str = crypto.b64decode(auth)
            expected_hash = crypto.hashString(
                'sha256', crypto.secret, crypto.secret_key)

            hash_challenge = crypto.checkHashString(hash_str, expected_hash)

            if hash_challenge:
                query = 'SELECT CODE_OPERATEUR, FONCTION FROM T_OPERATEUR'
                operators = Query(*CURRENT_CONFIG).executeQuery(query)
                return operators

        return {}


class Token(Resource):
    def post(self):
        jwt = request.data.decode('ansi')

        if jwt:
            crypto = Crypto(SECRET)
            payload = crypto.readJWT(jwt)
            return payload

        return {}
