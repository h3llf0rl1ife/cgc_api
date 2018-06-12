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


class ResfulQuery(Resource):
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
                column = [*data['Parameters']['Where']][0]
            except KeyError:
                return {'Status': 400, 'Message': 'Bad request.', 'JSON Data': data}, 400

            try:
                column = self._Query.validateColumn(table, column)
            except pymssql.OperationalError:
                return {'Status': 503, 'Message': 'Database connection failed.'}, 503

            try:
                value = data['Parameters']['Where'][column]
            except KeyError:
                params = {'Table': table, 'Column': [*data['Parameters']['Where']][0]}
                return {'Status': 400, 'Message': 'Column not found.', 'Parameters': params}, 400
            
        query = self._Query.getRequest(table, column)

        try:
            return self._Query.executeQuery(query=query, param=value), 200
    
        except pymssql.OperationalError:
            params = {'Table': table, 'Column': column, 'Value': value}
            return {'Status': 400, 'Message': 'Error during query execution.', 'Parameters': params}, 400

        except pymssql.ProgrammingError:
            return {'Status': 500, 'Message': 'Error during query execution.'}, 500


    def post(self, table):
        pass
    

    def put(self, table):
        pass

    
    def delete(self, table):
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
                column = [*data['Parameters']['Where']][0]
            except KeyError:
                return {'Status': 400, 'Message': 'Bad request.', 'JSON Data': data}, 400

            try:
                column = self._Query.validateColumn(table, column)
            except pymssql.OperationalError:
                return {'Status': 503, 'Message': 'Database connection failed.'}, 503

            try:
                value = data['Parameters']['Where'][column]
            except KeyError:
                params = {'Table': table, 'Column': [*data['Parameters']['Where']][0]}
                return {'Status': 400, 'Message': 'Column not found.', 'Parameters': params}, 400
            
        query = self._Query.deleteRequest(table, column)

        try:
            row_count = self._Query.executeQuery(query=query, param=value, with_result=False)
            return {'Status': 200, 'Message': 'Deleted {} records from {}.'.format(row_count, table)}, 200

        except pymssql.OperationalError:
            params = {'Table': table, 'Column': column, 'Value': value}
            return {'Status': 400, 'Message': 'Error during query execution.', 'Parameters': params}, 400

        except pymssql.ProgrammingError:
            return {'Status': 500, 'Message': 'Error during query execution.'}, 500
