from flask import Flask, Blueprint
from flask_restful import Api

from cgc_api import resources as r
from cgc_api.config import DEBUG_CONFIG 


app = Flask(__name__)

# Blueprints
api_bp = Blueprint('api', __name__, url_prefix='/api/v1')

# APIs
api = Api(api_bp)

# Add resources
api.add_resource(r.APIRequest, '/queries')
api.add_resource(r.RestfulQuery, '/query/<string:table>')
api.add_resource(r.RestfulSchema, '/schema', '/')

# Register blueprints
app.register_blueprint(api_bp)

# Debugging
if __name__ == '__main__':
    app.run(**DEBUG_CONFIG)
