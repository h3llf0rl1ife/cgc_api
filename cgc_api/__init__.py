from flask import Flask, Blueprint
from flask_restful import Api

from cgc_api import resources as r
from cgc_api.config import DEBUG_CONFIG 


app = Flask(__name__)

# Blueprints
api_bp = Blueprint('api', __name__, url_prefix='/api')

# APIs
api_v1 = Api(api_bp, '/v1')
api_v0 = Api(api_bp, '/v0')

# Add resources
api_v1.add_resource(r.APIRequest, '/queries')
api_v1.add_resource(r.RestfulQuery, '/query/<string:table>')
api_v1.add_resource(r.RestfulSchemaV1, '/schema', '/')

api_v0.add_resource(r.TemporaryRedirect, '/query/<string:table>')
api_v0.add_resource(r.RestfulSchemaV0, '/schema', '/')

# Register blueprints
app.register_blueprint(api_bp)

# Debugging
if __name__ == '__main__':
    app.run(**DEBUG_CONFIG)
