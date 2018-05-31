from flask import Flask, Blueprint
from flask_restful import Api
from cgc_api import resources as r

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
import inspect


app = Flask(__name__)

# Database mapping
Base = automap_base()
engine = create_engine("mssql+pymssql://sqladmin:AcChRgHax2C0p3s@10.7.2.1/AGENCE_TAROUDANT?charset=utf8")
Base.prepare(engine, reflect=True)

# Blueprints
api_bp = Blueprint('api', __name__)

# APIs
api = Api(api_bp)

# Add resources
api.add_resource(r.APIRequest, '/api')
api.add_resource(r.RestfulRequest, '/api/<string:table>')

# Register blueprints
app.register_blueprint(api_bp)

# Debugging
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
