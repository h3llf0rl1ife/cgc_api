from flask import Flask, Blueprint
from flask_restful import Api
from flask_sqlalchemy import SQLAlchemy

from cgc_api.config import CURRENT_CONFIG, DEBUG_CONFIG, DATABASE_URI


app = Flask(__name__)

# SQL Alchemy
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URI.format(
        CURRENT_CONFIG[1], CURRENT_CONFIG[2],
        CURRENT_CONFIG[0], CURRENT_CONFIG[3])
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Blueprints
api_bp = Blueprint('api', __name__, url_prefix='/api')
auth_bp = Blueprint('auth', __name__, url_prefix='/auth')

# APIs
api_v1 = Api(api_bp, '/v1')
auth_jwt = Api(auth_bp)

# Import resources
from cgc_api import auth as a
from cgc_api import resources as r

# Add resources
api_v1.add_resource(r.QueriesAPI, '/queries')
api_v1.add_resource(r.DatabaseAPI, '/query/<string:table>')
api_v1.add_resource(r.RestfulSchemaV0, '/schema', '/')
auth_jwt.add_resource(a.Auth, '/access')
auth_jwt.add_resource(a.Token, '/token')

# Register blueprints
app.register_blueprint(api_bp)
app.register_blueprint(auth_bp)

# Debugging
if __name__ == '__main__':
    app.run(**DEBUG_CONFIG)
