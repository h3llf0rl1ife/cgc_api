import logging
import datetime
from logging.config import dictConfig

from flask import Flask, Blueprint
from flask_restful import Api
from flask_sqlalchemy import SQLAlchemy

from cgc_api.config import CURRENT_CONFIG, DEBUG_CONFIG, DATABASE_URI


dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format':  '[{}] {} line {} in {}.{}: {}'.format(
            '%(asctime)s', '%(levelname)s', '%(lineno)d',
            '%(module)s', '%(funcName)s', '%(message)s')
    }},
    'handlers': {
        'wsgi': {
            'class': 'logging.StreamHandler',
            'stream': 'ext://flask.logging.wsgi_errors_stream',
            'formatter': 'default'
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'default',
            'filename': 'wsgi.log'
        },
        'daily': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'default',
            'filename': 'logs/daily.log',
            'when': 'midnight',
            'interval': 1,
            'utc': True,
            'delay': True
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['daily']
    }
})

app = Flask(__name__)

# SQL Alchemy
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URI.format(
    CURRENT_CONFIG[1], CURRENT_CONFIG[2],
    CURRENT_CONFIG[0], CURRENT_CONFIG[3])
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_POOL_SIZE'] = 25
app.config['SQLALCHEMY_MAX_OVERFLOW'] = -1
app.config['SQLALCHEMY_BINDS'] = {
    'stats': DATABASE_URI.format(
        CURRENT_CONFIG[1], CURRENT_CONFIG[2],
        CURRENT_CONFIG[0], 'STATISTIQUES')}
db = SQLAlchemy(app)

# Blueprints
api_bp = Blueprint('api', __name__, url_prefix='/api')
auth_bp = Blueprint('auth', __name__, url_prefix='/auth')

# APIs
api_v1 = Api(api_bp, '/v1')
api_v2 = Api(api_bp, '/v2')
auth_jwt = Api(auth_bp)

# Import resources
from cgc_api import auth as a
from cgc_api import resources as r
from cgc_api.api.v2 import resources as v2r

# Add resources
api_v1.add_resource(r.QueriesAPI, '/queries')
api_v1.add_resource(r.DatabaseAPI, '/query/<string:table>')
api_v1.add_resource(r.RestfulSchemaV1, '/schema', '/')
api_v2.add_resource(v2r.RestfulSchemaV2, '/schema', '/')
api_v2.add_resource(v2r.QueriesAPI_V2, '/queries')
api_v2.add_resource(v2r.DatabaseAPI_V2, '/query/<string:table>')
# api_v2.add_resource(v2r.Journee, '/journee/<int:agence>')
auth_jwt.add_resource(a.Auth, '/access')
auth_jwt.add_resource(a.Token, '/token')

# Register blueprints
app.register_blueprint(api_bp)
app.register_blueprint(auth_bp)

# Debugging
if __name__ == '__main__':
    app.run(**DEBUG_CONFIG)
