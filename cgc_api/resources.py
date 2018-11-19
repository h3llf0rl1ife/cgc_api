import re
from collections import OrderedDict

import pyodbc
from flask import request
from flask_restful import Resource

from cgc_api.queries import Queries
from cgc_api.query import Query
from cgc_api.config import CURRENT_CONFIG, HTTP_STATUS, SECRET, STAT_TABLES
from cgc_api import app
from cgc_api.crypto import Crypto
from cgc_api.auth import getDate


def removeSQLInjection(text):
    if not isinstance(text, str):
        return text

    chars = {';': '', '--': '', '/*': '', '*/': ''}
    rx = re.compile('|'.join(map(re.escape, chars)))

    def one_xlat(match):
        return chars[match.group(0)]

    return rx.sub(one_xlat, text)


class QueriesAPI(Resource):
    queries = Queries(*CURRENT_CONFIG)

    def get(self):
        pass

    def post(self):
        jwt = request.data.decode('utf-8')
        params, agency = None, 0

        if jwt:
            crypto = Crypto(SECRET + getDate())
            header, payload = crypto.readJWT(jwt)

            if payload:
                # token = payload.get('Token')

                """if token:
                    token = v1m.Token.query.filter_by(TokenHash=token).first()
                    if token:
                        if token.IssuedAt.date().isoformat() != getDate():
                            return {'Status': 498,
                                    'Message': 'Token expired'}, 498

                if not token:
                    return {'Status': 401,
                            'Message': 'Unauthorized access'}, 401"""

                params = payload.get('Parameters')
                agency = payload.get('Agency')

        if params:
            query = params.get('query')
            log = '{} - {} - Query: {} - Data: {}'.format(
                request.environ['REMOTE_ADDR'],
                type(self).__name__, params.get('query'),
                payload)

            if query:
                try:
                    query = getattr(self.queries, query)
                except AttributeError as e:
                    app.logger.warning(log)
                    app.log_exception(e)
                    return HTTP_STATUS['477'], 477

                kwargs = payload.get('Parameters').get('kwargs')

                if kwargs:
                    kwargs = [removeSQLInjection(kwarg) for kwarg in kwargs]
                    kwargs.append(agency)
                else:
                    kwargs = [agency]

                try:
                    return self.queries.executeQuery(query(kwargs))

                except (IndexError, ValueError) as e:
                    app.logger.warning(log)
                    app.log_exception(e)
                    return HTTP_STATUS['472'], 472

                except (pyodbc.ProgrammingError,
                        pyodbc.OperationalError) as e:
                    app.logger.warning(log)
                    app.log_exception(e)
                    return HTTP_STATUS['488'], 488

                except pyodbc.IntegrityError as e:
                    app.logger.warning(log)
                    app.log_exception(e)
                    return HTTP_STATUS['487'], 487

        return HTTP_STATUS['471'], 471


class RestfulQuery:
    _Query = Query(*CURRENT_CONFIG)

    def __init__(self, data):
        self.data = data

    def get(self, table):
        data = self.data
        column, value = None, None
        log = '{} - {} - Table: {} - Data: {}'.format(
                request.environ['REMOTE_ADDR'], type(self).__name__,
                table, data)

        if data:
            try:
                column = [*data['Parameters']['Select']['Where']][0]
            except (KeyError, IndexError) as e:
                app.logger.warning(log)
                app.log_exception(e)
                return HTTP_STATUS['471'], 471

            try:
                value = data['Parameters']['Select']['Where'][column]
            except KeyError as e:
                app.logger.warning(log)
                app.log_exception(e)
                return HTTP_STATUS['475'], 475

        query = self._Query.getRequest(table, column)

        try:
            param = (value, ) if value else value
            return self._Query.executeQuery(query=query, param=param), 200

        except pyodbc.OperationalError as e:
            app.logger.warning(log)
            app.log_exception(e)
            return HTTP_STATUS['473'], 473

        except pyodbc.ProgrammingError as e:
            app.logger.warning(log)
            app.log_exception(e)
            return HTTP_STATUS['488'], 488

    def post(self, table):
        data = self.data
        log = '{} - {} - Table: {} - Data: {}'.format(
                request.environ['REMOTE_ADDR'],
                type(self).__name__, table, data)

        try:
            columns = [*data['Parameters']['Insert']['Values']]
        except (TypeError, KeyError) as e:
            app.logger.warning(log)
            app.log_exception(e)
            return HTTP_STATUS['471'], 471

        values = data['Parameters']['Insert']['Values']
        values = tuple([values[column] for column in columns])
        query = self._Query.postRequest(table, columns)
        if table == 'P_TIERS':
            query = 'SET IDENTITY_INSERT P_TIERS ON\n \
                     {}\nSET IDENTITY_INSERT P_TIERS OFF'.format(query)

        try:
            row_count = self._Query.executeQuery(
                query=query, param=values, with_result=False)

            return {'Status': 200,
                    'Message': 'Inserted {} record into {}.'.format(
                        row_count, table)}, 200

        except pyodbc.OperationalError as e:
            app.logger.warning(log)
            app.log_exception(e)
            return HTTP_STATUS['471'], 471

        except pyodbc.ProgrammingError as e:
            app.logger.warning(log)
            app.log_exception(e)
            return HTTP_STATUS['488'], 488

        except pyodbc.IntegrityError as e:
            app.logger.warning(log)
            app.log_exception(e)
            return HTTP_STATUS['487'], 487

    def put(self, table):
        data = self.data
        log = '{} - {} - Table: {} - Data: {}'.format(
                request.environ['REMOTE_ADDR'],
                type(self).__name__, table, data)

        try:
            u_columns = [*data['Parameters']['Update']['Values']]
            w_columns = data['Parameters']['Update']['Where']
        except (KeyError, TypeError) as e:
            app.logger.warning(log)
            app.log_exception(e)
            return HTTP_STATUS['471'], 471

        data_type = w_columns[[*w_columns][0]]

        if isinstance(data_type, dict):
            w_columns = OrderedDict(w_columns)
        else:
            w_columns = [*data['Parameters']['Update']['Where']]

        u_values = data['Parameters']['Update']['Values']
        u_values = tuple([u_values[column] if u_values[column] != 'NULL'
                          else None for column in u_columns])

        w_values = data['Parameters']['Update']['Where']

        if isinstance(data_type, dict):
            w_values_ = ()
            for operator in w_columns:
                for column in w_columns[operator]:
                    w_values_ += (w_values[operator][column], )
            w_values = w_values_
            kwargs = w_columns
        else:
            w_values = tuple([w_values[column] for column in w_columns])
            kwargs = {'w_columns': w_columns}

        values = u_values + w_values
        query = self._Query.putRequest(table, u_columns, **kwargs)

        try:
            row_count = self._Query.executeQuery(
                query=query, param=values, with_result=False)
            return {'Status': 200,
                    'Message': 'Updated {} record in {}.'.format(
                        row_count, table)}, 200

        except pyodbc.OperationalError as e:
            app.logger.warning(log)
            app.log_exception(e)
            return HTTP_STATUS['471'], 471

        except pyodbc.ProgrammingError as e:
            app.logger.warning(log)
            app.log_exception(e)
            return HTTP_STATUS['488'], 488

    def delete(self, table):
        data = self.data
        columns, values, kwargs = None, None, {}
        log = '{} - {} - Table: {} - Data: {}'.format(
                request.environ['REMOTE_ADDR'],
                type(self).__name__, table, data)

        if data is not None:
            try:
                columns = data['Parameters']['Delete']['Where']
            except KeyError as e:
                app.logger.warning(log)
                app.log_exception(e)
                return HTTP_STATUS['471'], 471

            data_type = columns[[*columns][0]]

            if isinstance(data_type, dict):
                columns = OrderedDict(columns)
            else:
                columns = [*data['Parameters']['Delete']['Where']]

            try:
                values = data['Parameters']['Delete']['Where']

                if isinstance(data_type, dict):
                    values_ = ()
                    for operator in columns:
                        for column in columns[operator]:
                            values_ += (values[operator][column], )
                    values = values_
                    kwargs = columns
                else:
                    values = tuple([values[column] for column in columns])
                    kwargs = {'columns': columns}
            except KeyError as e:
                app.logger.warning(log)
                app.log_exception(e)
                return HTTP_STATUS['475'], 475

        query = self._Query.deleteRequest(table, **kwargs)

        try:
            row_count = self._Query.executeQuery(
                query=query, param=values, with_result=False)

            return {'Status': 200,
                    'Message': 'Deleted {} records from {}.'.format(
                        row_count, table)}, 200

        except pyodbc.OperationalError as e:
            app.logger.warning(log)
            app.log_exception(e)
            return HTTP_STATUS['471'], 471

        except pyodbc.ProgrammingError as e:
            app.logger.warning(log)
            app.log_exception(e)
            return HTTP_STATUS['488'], 488


class DatabaseAPI(Resource):
    def respond(self, table):
        jwt = request.data.decode('utf-8')
        params = None

        if jwt:
            crypto = Crypto(SECRET + getDate())
            header, payload = crypto.readJWT(jwt)
            log = '{} {} Data: {}'.format(
                request.environ['REMOTE_ADDR'],
                type(self).__name__, payload)

            if payload:
                # token = payload.get('Token')

                """if token:
                    token = v1m.Token.query.filter_by(TokenHash=token).first()
                    if token:
                        if token.IssuedAt.date().isoformat() != getDate():
                            return {'Status': 498,
                                    'Message': 'Token expired'}, 498

                if not token:
                    return {'Status': 401,
                            'Message': 'Unauthorized access'}, 401"""

                params = payload.get('Parameters')

            if params:
                params = {'Parameters': params}

            restfulQuery = RestfulQuery(params)

            if table in STAT_TABLES:
                args = CURRENT_CONFIG[:3] + ('STATISTIQUES',)
                restfulQuery._Query = Query(*args)
            elif table == 'P_TIERS':
                args = ('10.7.0.20',) + CURRENT_CONFIG[1:3] + ('GCOPAG',)
                restfulQuery._Query = Query(*args)
            elif table == 'TMPS_CLIENTS':
                args = CURRENT_CONFIG[:3] + ('SIEGE',)
                restfulQuery._Query = Query(*args)
            else:
                restfulQuery._Query = Query(*CURRENT_CONFIG)

            methods = {
                'GET': restfulQuery.get,
                'POST': restfulQuery.post,
                'PUT': restfulQuery.put,
                'DELETE': restfulQuery.delete
            }
            method = payload.get('Method')

            try:
                response, status_code = methods[method](table)
            except KeyError as e:
                app.logger.warning(log)
                app.log_exception(e)
                return HTTP_STATUS['405'], 405

            return response, status_code

        return HTTP_STATUS['471'], 471

    def get(self, table):
        return self.respond(table)

    def post(self, table):
        return self.respond(table)


class RestfulSchemaV1(Resource):
    def get(self):
        schema = {
            'Version': 1,
            'URI': '/api/v1',
            'Resources': {
                '/queries': {
                    'Parameters': {
                        'query': '<query name>',
                        'kwargs': ['arg1', 'arg2']
                    },
                    'Token': '<Token>'
                },
                '/query/<table>': {
                    'Method': '<HTTP Method>',
                    'Parameters': {
                        'Select': {
                            'Where': {
                                '<column>': '<value>'
                            }
                        },
                        'Insert': {
                            'Values': {
                                '<column>': '<value>'
                            }
                        },
                        'Update': {
                            'Values': {
                                '<column>': '<value>'
                            },
                            'Where': {
                                '<column>': '<value>',
                                '<operator>': {
                                    '<column>': '<value>'
                                }
                            }
                        },
                        'Delete': {
                            'Where': {
                                '<column>': '<value>',
                                '<operator>': {
                                    '<column>': '<value>'
                                }
                            }
                        }
                    },
                    'Token': '<Token>'
                }
            },
            'HTTP Methods': ['GET', 'POST', 'PUT', 'DELETE'],
            'Operators': {
                'Equal': '=',
                'NotEqual': ['!=', '<>'],
                'GreaterThan': '>',
                'GreaterEqual': '>=',
                'LessThan': '<',
                'LessEqual': '<=',
                'Like': 'LIKE',
                'ILike': 'ILIKE',
                'NotLike': 'NOT LIKE',
                'NotILike': 'NOT ILIKE'
            }
        }
        return schema
