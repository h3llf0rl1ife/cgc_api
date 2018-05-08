from flask import request
from flask_restful import Resource
from cgc_api.queries import Queries


class APIRequest(Resource):
    queries = Queries("192.168.64.234", "gestcom", "miftah", "Eljadida")


    def get(self):
        pass


    def post(self):
        keys = {
            'username': "OA6iJ1qRtmfnLzJyFRDJROfJahq8B5Kn",
            'password': "3AisU6jiTgvsO9YcMtc0CYQpqDVhmxCN"
        }
        auth = request.authorization

        if auth != None:
            if auth.username != keys['username'] or auth.password != keys['password']:
                return {'Status': 401}
        else:
            return {'Status': 401}


        data = request.get_json()
        print('data', data)
        
        try:
            query = getattr(self.queries, data['Parameters']['query'])
            #print(query)
        except (TypeError, KeyError):
            return {'status': 400}

        kwargs = data['Parameters']['kwargs']

        #print('kwargs', kwargs)
        print('querykwargs', query(kwargs))

        try:
            return self.queries.executeQuery(query(kwargs))
        except IndexError:
            raise
        
        return {'status': 400}
 