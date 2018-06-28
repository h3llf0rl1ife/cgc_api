from flask import Flask, Blueprint
from flask_restful import Api
from flask_sqlalchemy import SQLAlchemy

from cgc_api import resources as r
from cgc_api.config import CURRENT_CONFIG, DEBUG_CONFIG, DATABASE_URI


app = Flask(__name__)

# SQL Alchemy
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URI.format(
        CURRENT_CONFIG[1], CURRENT_CONFIG[2],
        CURRENT_CONFIG[0], CURRENT_CONFIG[3])
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

from cgc_api import models

db.create_all()
db.session.commit()

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
