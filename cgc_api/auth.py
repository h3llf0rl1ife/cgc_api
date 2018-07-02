import os
import datetime

from flask import request
from flask_restful import Resource

from cgc_api.config import CURRENT_CONFIG, SECRET
from cgc_api.query import Query
from cgc_api.crypto import Crypto
from cgc_api import models as m
from cgc_api import db

sqlserver = Query(*CURRENT_CONFIG)


def getDate():
    return datetime.date.today().isoformat()


class Auth(Resource):
    def get(self):
        return {'Date': getDate()}

    def post(self):
        auth = request.data

        if auth:
            crypto = Crypto(SECRET + getDate())
            hash_str = crypto.b64decode(auth)
            expected_hash = crypto.hashString(
                'sha512', crypto.secret, crypto.secret_key)

            hash_challenge = crypto.checkHashString(hash_str, expected_hash)

            if hash_challenge:
                query = 'SELECT CODE_OPERATEUR, FONCTION FROM T_OPERATEUR'
                operators = sqlserver.executeQuery(query)
                return operators

        return {}


class Token(Resource):
    def post(self):
        jwt = request.data.decode('ansi')
        password_check = None

        if jwt:
            crypto = Crypto(SECRET + getDate())
            payload = crypto.readJWT(jwt)

            if payload:
                machine = m.Machine.query.filter_by(
                    MachineName=payload['Machine']).first()
                operator = m.Operator.query.filter_by(
                    OperatorCode=payload['Operator']).first()

                if operator:
                    salt = crypto.unhexlify(bytearray(operator.Salt, 'utf-8'))
                    password = crypto.unhexlify(
                        bytearray(operator.Password, 'utf-8'))
                    p_password = crypto.hashString(
                        'sha256', bytearray(
                            payload['Password'], 'utf-8'), salt=salt)
                    password_check = crypto.checkHashString(
                        password, p_password)

                if machine and password_check:
                    token = m.Token.query.filter_by(
                        Machine=machine, Operator=operator,
                        IssuedAt=getDate(), ExpiresAt=getDate()).first()

                    if token:
                        return {'Token': token.TokenHash}

                    token_hash = crypto.generateToken(64)
                    token = m.Token(
                        TokenHash=token_hash, Active=True,
                        Machine=machine, Operator=operator,
                        IssuedAt=getDate(), ExpiresAt=getDate())
                    db.session.add(token)
                    db.session.commit()

                    return {'Token': token_hash}

        return {}
