import re
import datetime
from collections import OrderedDict

import pymssql
from flask import request
from flask_restful import Resource

from cgc_api.queries import Queries
from cgc_api.query import Query
from cgc_api.config import CURRENT_CONFIG, STAT_TABLES, HTTP_STATUS
from cgc_api import models as m


def getDate():
    return datetime.date.today().isoformat()


def removeSQLInjection(text):
    if not isinstance(text, str):
        return text

    chars = {';': '', '--': '', '\'': '', '/': '', '/': '', '*': ''}
    rx = re.compile('|'.join(map(re.escape, chars)))

    def one_xlat(match):
        return chars[match.group(0)]

    return rx.sub(one_xlat, text)


class QueriesAPI(Resource):
    queries = Queries(*CURRENT_CONFIG)

    def get(self):
        pass

    def post(self):
        data = request.get_json()

        if data:
            token = data.get('Token')

            if token:
                token = m.Token.query.filter_by(TokenHash=token).first()
                if token:
                    if token.IssuedAt.date().isoformat() != getDate():
                        return {'Status': 498,
                                'Message': 'Token expired.'}, 498

            if not token:
                return {'Status': 401,
                        'Message': 'Token authentication failed.'}, 401

        try:
            query = getattr(self.queries, data['Parameters']['query'])
        except AttributeError:
            return HTTP_STATUS['477'], 477
        except (KeyError, TypeError):
            return HTTP_STATUS['477'], 477

        kwargs = data['Parameters']['kwargs']
        kwargs = [removeSQLInjection(kwarg) for kwarg in kwargs]

        try:
            return self.queries.executeQuery(query(kwargs))

        except (IndexError, ValueError):
            return HTTP_STATUS['472'], 472

        except (pymssql.ProgrammingError, pymssql.OperationalError):
            query = query(kwargs).replace(
                '\t', ' ').replace('\n', ' ')
            return HTTP_STATUS['488'], 488

        return HTTP_STATUS['400'], 400


class RestfulQuery:
    _Query = Query(*CURRENT_CONFIG)

    def __init__(self, data):
        self.data = data

    def get(self, table):
        try:
            table = self._Query.validateTable(table)
        except KeyError:
            return HTTP_STATUS['476'], 476

        except pymssql.OperationalError:
            return HTTP_STATUS['489'], 489

        data = self.data
        column, value = None, None

        if data:  # Make sure
            try:
                column = [*data['Parameters']['Select']['Where']][0]
            except (KeyError, IndexError):
                return HTTP_STATUS['471'], 471

            try:
                column = self._Query.validateColumn(table, column)
            except pymssql.OperationalError:
                return HTTP_STATUS['489'], 489

            try:
                value = data['Parameters']['Select']['Where'][column]
            except KeyError:
                return HTTP_STATUS['475'], 475

        query = self._Query.getRequest(table, column)

        try:
            return self._Query.executeQuery(query=query, param=value), 200

        except pymssql.OperationalError:
            return HTTP_STATUS['473'], 473

        except pymssql.ProgrammingError:
            return HTTP_STATUS['488'], 488

    def post(self, table):
        try:
            table = self._Query.validateTable(table)
        except KeyError:
            return HTTP_STATUS['476'], 476

        except pymssql.OperationalError:
            return HTTP_STATUS['489'], 489

        data = self.data

        try:
            columns = [*data['Parameters']['Insert']['Values']]
        except (TypeError, KeyError):
            return HTTP_STATUS['471'], 471

        n_columns = len(columns)

        try:
            columns = self._Query.validateColumn(table, columns, is_list=True)
        except pymssql.OperationalError:
            return HTTP_STATUS['489'], 489

        if n_columns != len(columns):
            return HTTP_STATUS['475'], 475

        values = data['Parameters']['Insert']['Values']
        values = tuple([values[column] for column in columns])
        query = self._Query.postRequest(table, columns)

        try:
            row_count = self._Query.executeQuery(
                query=query, param=values, with_result=False)

            return {'Status': 200,
                    'Message': 'Inserted {} record into {}.'.format(
                        row_count, table)}, 200

        except pymssql.OperationalError:
            return HTTP_STATUS['471'], 471

        except pymssql.ProgrammingError:
            return HTTP_STATUS['488'], 488

    def put(self, table):
        try:
            table = self._Query.validateTable(table)
        except KeyError:
            return HTTP_STATUS['476'], 476

        except pymssql.OperationalError:
            return HTTP_STATUS['489'], 489

        data = self.data

        try:
            u_columns = [*data['Parameters']['Update']['Values']]
            w_columns = data['Parameters']['Update']['Where']
        except (KeyError, TypeError):
            return HTTP_STATUS['471'], 471

        data_type = w_columns[[*w_columns][0]]

        if isinstance(data_type, dict):
            w_columns = OrderedDict(w_columns)
            n_w_columns = 0

            for operator in w_columns:
                n_w_columns += len(w_columns[operator])

            validation_kwargs = {'is_dict': True}

        else:
            w_columns = [*data['Parameters']['Update']['Where']]
            n_w_columns = len(w_columns)
            validation_kwargs = {'is_list': True}

        n_u_columns = len(u_columns)

        try:
            u_columns = self._Query.validateColumn(
                                  table, u_columns, is_list=True)
            w_columns = self._Query.validateColumn(
                                  table, w_columns, **validation_kwargs)
        except pymssql.OperationalError:
            return HTTP_STATUS['489'], 489

        if isinstance(data_type, dict):
            n_w_columns_ = 0
            for operator in w_columns:
                n_w_columns_ += len(w_columns[operator])
        else:
            n_w_columns_ = len(w_columns)

        if n_u_columns != len(u_columns) or n_w_columns != n_w_columns_:
            return HTTP_STATUS['475'], 475

        u_values = data['Parameters']['Update']['Values']
        u_values = tuple([u_values[column] for column in u_columns])

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

        except pymssql.OperationalError:
            return HTTP_STATUS['471'], 471

        except pymssql.ProgrammingError:
            return HTTP_STATUS['488'], 488

    def delete(self, table):
        try:
            table = self._Query.validateTable(table)
        except KeyError:
            return HTTP_STATUS['476'], 476

        except pymssql.OperationalError:
            return HTTP_STATUS['489'], 489

        data = self.data
        columns, values, kwargs = None, None, {}

        if data is not None:
            try:
                columns = data['Parameters']['Delete']['Where']
            except KeyError:
                return HTTP_STATUS['471'], 471

            data_type = columns[[*columns][0]]

            if isinstance(data_type, dict):
                columns = OrderedDict(columns)
                validation_kwargs = {'is_dict': True}

            else:
                columns = [*data['Parameters']['Delete']['Where']]
                validation_kwargs = {'is_list': True}

            try:
                columns = self._Query.validateColumn(
                                table, columns, **validation_kwargs)
            except pymssql.OperationalError:
                return HTTP_STATUS['489'], 489

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
            except KeyError:
                return HTTP_STATUS['475'], 475

        query = self._Query.deleteRequest(table, **kwargs)

        try:
            row_count = self._Query.executeQuery(
                query=query, param=values, with_result=False)

            return {'Status': 200,
                    'Message': 'Deleted {} records from {}.'.format(
                        row_count, table)}, 200

        except pymssql.OperationalError:
            return HTTP_STATUS['471'], 471

        except pymssql.ProgrammingError:
            return HTTP_STATUS['488'], 488


class DatabaseAPI(Resource):
    def post(self, table):
        data = request.get_json()

        if data:
            token = data.get('Token')

            if token:
                token = m.Token.query.filter_by(TokenHash=token).first()

                if token:
                    if token.IssuedAt != getDate():
                        return {'Status': 498,
                                'Message': 'Token expired.'}, 498

            if not token:
                return {'Status': 401,
                        'Message': 'Token authentication failed.'}, 401

        params = data.get('Parameters')

        if params:
            params = {'Parameters': params}

        restfulQuery = RestfulQuery(params)
        methods = {
            'GET': restfulQuery.get,
            'POST': restfulQuery.post,
            'PUT': restfulQuery.put,
            'DELETE': restfulQuery.delete
        }
        method = data.get('Method')

        try:
            response, status_code = methods[method](table)
        except KeyError:
            return HTTP_STATUS['405'], 405

        print(response)
        return response, status_code

    def get(self, table):
        return self.respond(table)

    def post(self, table):
        return self.respond(table)


class RestfulSchemaV0(Resource):
    def get(self):
        schema = {
            'Version': 1,
            'Resources': {
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
