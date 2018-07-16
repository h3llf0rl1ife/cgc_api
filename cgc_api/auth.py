import datetime

from flask import request
from flask_restful import Resource

from cgc_api.config import CURRENT_CONFIG, SECRET, JWT_HEADER
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
        auth = request.get_json()

        if auth:
            crypto = Crypto(SECRET + getDate())
            hash_str = crypto.b64decode(auth.get('Token'))
            expected_hash = crypto.hashString(
                'sha512', crypto.secret, crypto.secret_key)

            hash_challenge = crypto.checkHashString(hash_str, expected_hash)

            if hash_challenge:
                operator = '%{}%'.format(auth.get('Operator'))
                operators = m.Operateur.query.filter(
                    m.Operateur.Task_ != None,
                    m.Operateur.Active == 1,
                    m.Operateur.Function > 5,
                    m.Operateur.OperatorName.like(operator),
                    m.Operateur.AgencyCode == auth.get('Agency')).all()

                return [{'CODE_OPERATEUR': op.OperatorCode,
                         'NOM_OPERATEUR': op.OperatorName}
                        for op in operators], 200

        return {}, 400


class Token(Resource):
    def post(self):
        jwt = request.data.decode('utf-8')
        password_check = False

        if jwt:
            crypto = Crypto(SECRET + getDate())
            header, payload = crypto.readJWT(jwt)

            if payload:
                machine_name = payload.get('Machine')

                machine = m.Machine.query.filter_by(
                    MachineName=machine_name).first()

                if not machine:
                    machine = m.Machine(MachineName=machine_name, Active=True)
                    db.session.add(machine)
                    db.session.commit()

                operator = m.Operator.query.filter_by(
                    OperatorCode=payload.get('Operator')).first()

                if operator:
                    if operator.Operateur_.Task_:
                        salt = crypto.unhexlify(
                            bytearray(operator.Salt, 'utf-8'))
                        password = crypto.unhexlify(
                            bytearray(operator.Password, 'utf-8'))
                        p_password = crypto.hashString(
                            'sha256', bytearray(
                                payload.get('Password'), 'utf-8'), salt=salt)
                        password_check = crypto.checkHashString(
                            password, p_password)

                if password_check:
                    token = m.Token.query.filter_by(
                        Machine=machine, Operator=operator,
                        IssuedAt=getDate(), ExpiresAt=getDate()).first()

                    if token:
                        token_hash = token.TokenHash
                    else:
                        token_hash = crypto.generateToken(128)
                        token = m.Token(
                            TokenHash=token_hash, Active=True,
                            Machine=machine, Operator=operator,
                            IssuedAt=getDate(), ExpiresAt=getDate())
                        db.session.add(token)
                        db.session.commit()

                    payload = {'Token': token_hash}
                    header = JWT_HEADER
                    jwt = crypto.writeJWT(header, payload)
                    print(token_hash)

                    return {'Token': jwt}, 200

        return {}, 400
