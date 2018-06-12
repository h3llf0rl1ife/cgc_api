from flask import Flask, Blueprint
from flask_restful import Api

app = Flask(__name__)

# Import resources
from cgc_api import resources as r

# Blueprints
api_bp = Blueprint('api', __name__)

# APIs
api = Api(api_bp)

# Add resources
api.add_resource(r.APIRequest, '/api')
api.add_resource(r.ResfulQuery, '/api/<string:table>')

# Register blueprints
app.register_blueprint(api_bp)

# Debugging
if __name__ == '__main__':
    app.run(debug=True) #host='0.0.0.0', port=80, 
