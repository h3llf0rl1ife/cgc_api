from flask import Flask, Blueprint
from flask_restful import Api

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
import inspect


app = Flask(__name__)

# Database mapping
Base = automap_base()
engine = create_engine("mssql+pymssql://sqladmin:AcChRgHax2C0p3s@10.7.2.1/TEST_SIEGE?charset=utf8")
Base.prepare(engine, reflect=True)

# Import resources after mapping the database
from cgc_api import resources as r

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
    app.run(debug=True) #host='0.0.0.0', port=80, 
