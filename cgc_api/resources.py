from flask import request
from flask_restful import Resource
from cgc_api.queries import Queries


class APIRequest(Resource):
    def get(self):
        return "Ok"

    def put(self):
        pass
    
    def post(self):
        data = request.get_json()
        print(data)
        
        query = getattr(Queries, data['query'])
        print(query)
        
        kwargs = data['kwargs']
        print(kwargs)

        for key in kwargs:
            kwargs[key] = 0 if kwargs[key] == '' else kwargs[key]

        print(query(kwargs))
        return {'status': 200}
    
    def delete(self):
        pass
