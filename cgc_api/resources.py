from flask import request
from flask_restful import Resource
from cgc_api.queries import Queries
import pymssql


class APIRequest(Resource):
    queries = Queries("10.7.2.1", "sqladmin", "AcChRgHax2C0p3s", "TEST_SIEGE")


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
 