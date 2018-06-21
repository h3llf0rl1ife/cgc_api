from collections import OrderedDict

import pymssql
import requests
from flask import request
from flask_restful import Resource

from cgc_api.queries import Queries
from cgc_api.query import Query
from cgc_api.config import CURRENT_CONFIG


class APIRequest(Resource):
    queries = Queries(*CURRENT_CONFIG)

    def get(self):
        pass

    def post(self):
        keys = {
            'username': "OA6iJ1qRtmfnLzJyFRDJROfJahq8B5Kn",
            'password': "3AisU6jiTgvsO9YcMtc0CYQpqDVhmxCN"
        }
        auth = request.authorization

        if auth is not None:
            if (auth.username, auth.password) is not (
                    keys['username'], keys['password']):
                return {'Status': 401, 'Message': 'Unauthorized access.'}
        else:
            return {'Status': 401, 'Message': 'Unauthorized access.'}

        data = request.get_json()
        print('data', data)

        try:
            query = getattr(self.queries, data['Parameters']['query'])
        except AttributeError:
            return {'Status': 404, 'Message': 'Query not found.',
                    'Client data': data}
        except (KeyError, TypeError):
            return {'Status': 400, 'Message': 'Bad request.',
                    'Client data': data}

        # TODO: Add protection againt SQL injection

        kwargs = data['Parameters']['kwargs']
        print('query', query(kwargs))

        try:
            return self.queries.executeQuery(query(kwargs))

        except IndexError:
            return {'Status': 400, 'Message': 'One or more arguments missing.',
                    'Client data': data}

        except ValueError:
            raise

        except pymssql.ProgrammingError:
            query = query(kwargs).replace(
                '\t', '').replace('\n', '').replace('            ', '').strip()

            return {'Status': 400, 'Message': 'Error during query execution.',
                    'Query': query}

        return {'Status': 400, 'Message': 'Bad request.'}


class RestfulQuery(Resource):
    _Query = Query(*CURRENT_CONFIG)

    def get(self, table):
        try:
            table = self._Query.validateTable(table)
        except KeyError:
            return {'Status': 404, 'Message': 'Table not found.',
                    'Parameters': {'Table': table}}, 404

        except pymssql.OperationalError:
            return {'Status': 503,
                    'Message': 'Database connection failed.'}, 503

        data = request.get_json()
        print(data)
        column, value = None, None

        if data is not None:
            try:
                column = [*data['Parameters']['Select']['Where']][0]
            except KeyError:
                return {'Status': 400, 'Message': 'Bad request.',
                        'JSON Data': data}, 400

            try:
                column = self._Query.validateColumn(table, column)
            except pymssql.OperationalError:
                return {'Status': 503,
                        'Message': 'Database connection failed.'}, 503

            try:
                value = data['Parameters']['Select']['Where'][column]
            except KeyError:
                column = [*data['Parameters']['Select']['Where']][0]
                params = {'Table': table, 'Column': column}

                return {'Status': 400, 'Message': 'Column not found.',
                        'Parameters': params}, 400

        query = self._Query.getRequest(table, column)

        try:
            return self._Query.executeQuery(query=query, param=value), 200

        except pymssql.OperationalError:
            params = {'Table': table, 'Column': column, 'Value': value}
            return {'Status': 400, 'Message': 'Please verify your data.',
                    'Parameters': params}, 400

        except pymssql.ProgrammingError:
            return {'Status': 500,
                    'Message': 'Error during query execution.'}, 500

    def post(self, table):
        try:
            table = self._Query.validateTable(table)
        except KeyError:
            return {'Status': 404, 'Message': 'Table not found.',
                    'Parameters': {'Table': table}}, 404

        except pymssql.OperationalError:
            return {'Status': 503,
                    'Message': 'Database connection failed.'}, 503

        data = request.get_json()

        try:
            columns = [*data['Parameters']['Insert']['Values']]
        except KeyError:
            return {'Status': 400, 'Message': 'Bad request.',
                    'JSON Data': data}, 400

        except TypeError:
            return {'Status': 400,
                    'Message': 'Please provide insert values.'}, 400

        n_columns = len(columns)

        try:
            columns = self._Query.validateColumn(table, columns, is_list=True)
        except pymssql.OperationalError:
            return {'Status': 503,
                    'Message': 'Database connection failed.'}, 503

        if n_columns != len(columns):
            params = {'Table': table,
                      'Columns': [*data['Parameters']['Insert']['Values']]}

            return {'Status': 400,
                    'Message': 'No match found for provided columns.',
                    'Parameters': params}, 400

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
            params = {'Table': table,
                      'Values': data['Parameters']['Insert']['Values']}

            return {'Status': 400, 'Message': 'Please verify your data.',
                    'Parameters': params}, 400

        except pymssql.ProgrammingError:
            return {'Status': 500,
                    'Message': 'Error during query execution.'}, 500

    def put(self, table):
        try:
            table = self._Query.validateTable(table)
        except KeyError:
            return {'Status': 404, 'Message': 'Table not found.',
                    'Parameters': {'Table': table}}, 404

        except pymssql.OperationalError:
            return {'Status': 503,
                    'Message': 'Database connection failed.'}, 503

        data = request.get_json()

        try:
            u_columns = [*data['Parameters']['Update']['Values']]
            w_columns = data['Parameters']['Update']['Where']
        except (KeyError, TypeError):
            return {'Status': 400, 'Message': 'Bad request.',
                    'JSON Data': data}, 400

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
            return {'Status': 503,
                    'Message': 'Database connection failed.'}, 503

        if isinstance(data_type, dict):
            n_w_columns_ = 0
            for operator in w_columns:
                n_w_columns_ += len(w_columns[operator])
        else:
            n_w_columns_ = len(w_columns)

        if n_u_columns != len(u_columns) or n_w_columns != n_w_columns_:
            params = {
                'Table': table, 'Columns': {
                    'Values': [*data['Parameters']['Update']['Values']],
                    'Where': w_columns}
                    }
            return {'Status': 400,
                    'Message': 'No match found for provided columns.',
                    'Parameters': params}, 400

        u_values = data['Parameters']['Update']['Values']
        u_values = tuple([u_values[column] for column in u_columns])

        w_values = data['Parameters']['Update']['Where']

        if isinstance(data_type, dict):
            w_values_ = ()
            for operator in w_columns:
                for column in w_columns[operator]:
                    w_values_ += (w_values[operator][column], )
            w_values = w_values_
        else:
            w_values = tuple([w_values[column] for column in w_columns])

        values = u_values + w_values

        query = self._Query.putRequest(table, u_columns, **w_columns)

        try:
            row_count = self._Query.executeQuery(
                query=query, param=values, with_result=False)
            return {'Status': 200,
                    'Message': 'Updated {} record in {}.'.format(
                        row_count, table)}, 200

        except pymssql.OperationalError:
            params = {
                    'Table': table, 'Columns': {
                        'Values': data['Parameters']['Update']['Values'],
                        'Where': data['Parameters']['Update']['Where']}
                        }
            return {'Status': 400, 'Message': 'Please verify your data.',
                    'Parameters': params}, 400

        except pymssql.ProgrammingError:
            return {'Status': 500,
                    'Message': 'Error during query execution.'}, 500

    def delete(self, table):
        try:
            table = self._Query.validateTable(table)
        except KeyError:
            return {'Status': 404, 'Message': 'Table not found.',
                    'Parameters': {'Table': table}}, 404

        except pymssql.OperationalError:
            return {'Status': 503,
                    'Message': 'Database connection failed.'}, 503

        data = request.get_json()
        columns, values = None, None

        if data is not None:
            try:
                columns = [*data['Parameters']['Delete']['Where']]
            except KeyError:
                return {'Status': 400, 'Message': 'Bad request.',
                        'JSON Data': data}, 400

            try:
                columns = self._Query.validateColumn(
                                table, columns, is_list=True)
            except pymssql.OperationalError:
                return {'Status': 503,
                        'Message': 'Database connection failed.'}, 503

            try:
                values = data['Parameters']['Delete']['Where']
                values = tuple([values[column] for column in columns])
            except KeyError:
                params = {'Table': table,
                          'Columns': [*data['Parameters']['Delete']['Where']]}

                return {'Status': 400,
                        'Message': 'No match found for provided columns.',
                        'Parameters': params}, 400

        query = self._Query.deleteRequest(table, columns)

        try:
            row_count = self._Query.executeQuery(
                query=query, param=values, with_result=False)

            return {'Status': 200,
                    'Message': 'Deleted {} records from {}.'.format(
                        row_count, table)}, 200

        except pymssql.OperationalError:
            params = {'Table': table, 'Columns': columns, 'Values': values}
            return {'Status': 400, 'Message': 'Please verify your data.',
                    'Parameters': params}, 400

        except pymssql.ProgrammingError:
            return {'Status': 500,
                    'Message': 'Error during query execution.'}, 500


class RestfulSchemaV1(Resource):
    def get(self):
        schema = {
            'Version': 1,
            'Resources': {
                '/queries': {
                    'Parameters': {
                        'query': '<query>',
                        'kwargs': {
                            '<arg>': '<value>'
                        }
                    }
                },
                '/query/<table>': {
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
                                '<column>': '<value>'
                            }
                        }
                    }
                }
            }
        }
        return schema


class TemporaryRedirect(Resource):
    def post(self, table):
        json = request.get_json()

        methods = {
            'GET': requests.get,
            'POST': requests.post,
            'PUT': requests.put,
            'DELETE': requests.delete
        }

        url = 'http://localhost/api/v1/query/{}'.format(table)

        params = None
        method = 'GET'
        if json is not None:
            method = json['Method']
            if len(json['Parameters']) != 0:
                params = {'Parameters': json['Parameters']}

        kwargs = {'json': params}

        r = methods[method](url, **kwargs)

        print(r.json())
        return r.json(), r.status_code


class RestfulSchemaV0(Resource):
    def get(self):
        schema = {
            'Version': 0,
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
                                '<column>': '<value>'
                            }
                        }
                    }
                }
            }
        }
        return schema
