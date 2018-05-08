from flask import Flask, Blueprint
from flask_restful import Api
from cgc_api import resources as r


app = Flask(__name__)

# Blueprints
api_bp = Blueprint('api', __name__)

# APIs
api = Api(api_bp)

# Add resources
api.add_resource(r.APIRequest, '/api')

# Register blueprints
app.register_blueprint(api_bp)

# Debugging
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
