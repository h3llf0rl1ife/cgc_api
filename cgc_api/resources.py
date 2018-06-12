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


class RestfulRequest(Resource):
    def get(self, table):
        try:
            t = Base.classes[table]
        except KeyError:
            return {'Status': 400, 'Message': 'Bad request.'}, 400

        Session = sessionmaker(bind=engine)
        session = Session()

        output = session.query(t).all()

        #print(session.query(t).column_descriptions)
        #print(t.__table__.columns.keys())

        return_value = []
        for obj in output:
            entry = {}
            for key in t.__table__.columns.keys():
                entry[key] = getattr(obj, key)
            return_value.append(entry)

        return return_value
