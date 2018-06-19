from flask import request
from flask_restful import Resource
from cgc_api.queries import Queries
from cgc_api.query import Query
from cgc_api.config import CURRENT_CONFIG
import pymssql


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
            if (auth.username, auth.password) != (keys['username'], keys['password']):
                return {'Status': 401, 'Message': 'Unauthorized access.'}
        else:
            return {'Status': 401, 'Message': 'Unauthorized access.'}

        data = request.get_json()
        print('data', data)
        
        try:
            query = getattr(self.queries, data['Parameters']['query'])
        except AttributeError:
            return {'Status': 404, 'Message': 'Query not found.', 'Client data': data}
        except (KeyError, TypeError):
            return {'Status': 400, 'Message': 'Bad request.', 'Client data': data}

        kwargs = data['Parameters']['kwargs']
        print('query', query(kwargs))

        try:
            return self.queries.executeQuery(query(kwargs))
        except IndexError:
            return {'Status': 400, 'Message': 'One or more arguments missing.', 'Client data': data}
        except ValueError:
            raise
        except pymssql.ProgrammingError:
            return {'Status': 400, 'Message': 'Error during query execution.', 'Query': query(kwargs).replace('\t', '').replace('\n', '').replace('            ', '').strip()}
        
        return {'Status': 400, 'Message': 'Bad request.'}


class RestfulQuery(Resource):
    _Query = Query(*CURRENT_CONFIG)
    
    
    def get(self, table):
        try:
            table = self._Query.validateTable(table)
        except KeyError:
            return {'Status': 404, 'Message': 'Table not found.', 'Parameters': {'Table': table}}, 404
    
        except pymssql.OperationalError:
            return {'Status': 503, 'Message': 'Database connection failed.'}, 503
        
        data = request.get_json()
        column, value = None, None

        if data is not None:
            try:
                column = [*data['Parameters']['Select']['Where']][0]
            except KeyError:
                return {'Status': 400, 'Message': 'Bad request.', 'JSON Data': data}, 400

            try:
                column = self._Query.validateColumn(table, column)
            except pymssql.OperationalError:
                return {'Status': 503, 'Message': 'Database connection failed.'}, 503

            try:
                value = data['Parameters']['Select']['Where'][column]
            except KeyError:
                params = {'Table': table, 'Column': [*data['Parameters']['Select']['Where']][0]}
                return {'Status': 400, 'Message': 'Column not found.', 'Parameters': params}, 400
            
        query = self._Query.getRequest(table, column)

        try:
            return self._Query.executeQuery(query=query, param=value), 200
    
        except pymssql.OperationalError:
            params = {'Table': table, 'Column': column, 'Value': value}
            return {'Status': 400, 'Message': 'Error during query execution. Please verify your data.', 'Parameters': params}, 400

        except pymssql.ProgrammingError:
            return {'Status': 500, 'Message': 'Error during query execution.'}, 500


    def post(self, table):
        try:
            table = self._Query.validateTable(table)
        except KeyError:
            return {'Status': 404, 'Message': 'Table not found.', 'Parameters': {'Table': table}}, 404

        except pymssql.OperationalError:
            return {'Status': 503, 'Message': 'Database connection failed.'}, 503

        data = request.get_json()

        try:
            columns = [*data['Parameters']['Insert']['Values']]
        except KeyError:
            return {'Status': 400, 'Message': 'Bad request.', 'JSON Data': data}, 400
        
        except TypeError:
            return {'Status': 400, 'Message': 'Bad request. Please provide insert values.'}, 400
        
        n_columns = len(columns)

        try:
            columns = self._Query.validateColumn(table, columns, is_list=True)
        except pymssql.OperationalError:
            return {'Status': 503, 'Message': 'Database connection failed.'}, 503

        if n_columns != len(columns):
            params = {'Table': table, 'Columns': [*data['Parameters']['Insert']['Values']]}
            return {'Status': 400, 'Message': 'No match found for one or more provided columns.', 'Parameters': params}, 400

        values = tuple([data['Parameters']['Insert']['Values'][column] for column in columns])
        query = self._Query.postRequest(table, columns)

        try:
            row_count = self._Query.executeQuery(query=query, param=values, with_result=False)
            return {'Status': 200, 'Message': 'Inserted {} record into {}.'.format(row_count, table)}, 200

        except pymssql.OperationalError:
            params = {'Table': table, 'Values': data['Parameters']['Insert']['Values']}
            return {'Status': 400, 'Message': 'Error during query execution. Please verify your data.', 'Parameters': params}, 400

        except pymssql.ProgrammingError:
            return {'Status': 500, 'Message': 'Error during query execution.'}, 500
    

    def put(self, table):
        try:
            table = self._Query.validateTable(table)
        except KeyError:
            return {'Status': 404, 'Message': 'Table not found.', 'Parameters': {'Table': table}}, 404

        except pymssql.OperationalError:
            return {'Status': 503, 'Message': 'Database connection failed.'}, 503

        data = request.get_json()

        try:
            update_columns = [*data['Parameters']['Update']['Values']]
            where_columns = [*data['Parameters']['Update']['Where']]
        except (KeyError, TypeError):
            return {'Status': 400, 'Message': 'Bad request.', 'JSON Data': data}, 400
        
        n_u_columns, n_w_columns = len(update_columns), len(where_columns)

        try:
            update_columns = self._Query.validateColumn(table, update_columns, is_list=True)
            where_columns = self._Query.validateColumn(table, where_columns, is_list=True)
        except pymssql.OperationalError:
            return {'Status': 503, 'Message': 'Database connection failed.'}, 503

        for columns, n_columns in zip([update_columns, where_columns], [n_u_columns, n_w_columns]):
            if n_columns != len(columns):
                params = {
                    'Table': table, 'Columns': {
                    'Values': [*data['Parameters']['Update']['Values']],
                    'Where': [*data['Parameters']['Update']['Where']]
                }}
                return {'Status': 400, 'Message': 'No match found for one or more provided columns.', 'Parameters': params}, 400

        update_values = tuple([data['Parameters']['Update']['Values'][column] for column in update_columns])
        where_values = tuple([data['Parameters']['Update']['Where'][column] for column in where_columns])
        values = update_values + where_values
        
        query = self._Query.putRequest(table, update_columns, where_columns)

        try:
            row_count = self._Query.executeQuery(query=query, param=values, with_result=False)
            return {'Status': 200, 'Message': 'Updated {} record in {}.'.format(row_count, table)}, 200

        except pymssql.OperationalError:
            params = {
                    'Table': table, 'Columns': {
                    'Values': data['Parameters']['Update']['Values'],
                    'Where': data['Parameters']['Update']['Where']
                }}
            return {'Status': 400, 'Message': 'Error during query execution. Please verify your data.', 'Parameters': params}, 400

        except pymssql.ProgrammingError:
            return {'Status': 500, 'Message': 'Error during query execution.'}, 500

    
    def delete(self, table):
        try:
            table = self._Query.validateTable(table)
        except KeyError:
            return {'Status': 404, 'Message': 'Table not found.', 'Parameters': {'Table': table}}, 404

        except pymssql.OperationalError:
            return {'Status': 503, 'Message': 'Database connection failed.'}, 503

        data = request.get_json()
        columns, values = None, None

        if data is not None:
            try:
                columns = [*data['Parameters']['Delete']['Where']]
            except KeyError:
                return {'Status': 400, 'Message': 'Bad request.', 'JSON Data': data}, 400

            try:
                columns = self._Query.validateColumn(table, columns, is_list=True)
            except pymssql.OperationalError:
                return {'Status': 503, 'Message': 'Database connection failed.'}, 503

            try:
                values = tuple([data['Parameters']['Delete']['Where'][column] for column in columns])
            except KeyError:
                params = {'Table': table, 'Columns': [*data['Parameters']['Delete']['Where']]}
                return {'Status': 400, 'Message': 'No match found for one or more provided columns.', 'Parameters': params}, 400
            
        query = self._Query.deleteRequest(table, columns)

        try:
            row_count = self._Query.executeQuery(query=query, param=values, with_result=False)
            return {'Status': 200, 'Message': 'Deleted {} records from {}.'.format(row_count, table)}, 200

        except pymssql.OperationalError:
            params = {'Table': table, 'Columns': columns, 'Values': values}
            return {'Status': 400, 'Message': 'Error during query execution. Please verify your data.', 'Parameters': params}, 400

        except pymssql.ProgrammingError:
            return {'Status': 500, 'Message': 'Error during query execution.'}, 500
            return {'Status': 500, 'Message': 'Error during query execution.'}, 500


class RestfulSchema(Resource):
    def get(self):
        schema = {
            'Parameters': {
                'Insert': {
                    'Values': {
                        '<column>': '<value>'
                    }
                },
                'Select': {
                    'Where': {
                        '<column>': '<value>'
                    }
                },
                'Delete': {
                    'Where': {
                        '<column>': '<value>'
                    }
                },
                'Update': {
                    'Values': {
                        '<column>': '<value>'
                    },
                    'Where': {
                        '<column>': '<value>'
                    }
                }
            }
        }
        return schema
