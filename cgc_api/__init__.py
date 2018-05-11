from quart import Quart, Blueprint, request, jsonify
#from flask_restful import Api
from cgc_api import resources as r
from cgc_api.queries import Queries


app = Quart(__name__)

# Blueprints
#api_bp = Blueprint('api', __name__)
#test_bp = Blueprint('test', __name__)

# APIs
#api = Api(api_bp)

# Add resources
#api.add_resource(r.APIRequest, '/api')

# Register blueprints
#app.register_blueprint(api_bp)
#app.register_blueprint(test_bp)

# Test view function
@app.route('/test', methods=['GET', 'POST'])
async def test():
    queries = Queries("192.168.64.234", "gestcom", "miftah", "Eljadida")

    data = await request.get_json()
    print('data', data)
    
    try:
        query = getattr(queries, data['Parameters']['query'])
        #print(query)
    except AttributeError:
        print('first')
        return jsonify({'Status': 404, 'Message': 'Query not found.'})
    except (KeyError, TypeError):
        print('second')
        return jsonify({'Status': 400, 'Message': 'Bad request.'})

    kwargs = data['Parameters']['kwargs']

    #print('kwargs', kwargs)
    print('querykwargs', query(kwargs))

    try:
        print('third')
        return jsonify(queries.executeQuery(query(kwargs)))
    except IndexError:
        print('fourth')
        return jsonify({'Status': 400, 'Message': 'One or more arguments missing.'})
    except ValueError:
        raise
    
    print('last')
    return jsonify({'Status': 400, 'Message': 'Bad request.'})

@app.route('/hello')
async def hello():
    return 'hello'

# Debugging
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
