from gevent.pywsgi import WSGIServer
from cgc_api import app
from cgc_api.config import HOST_PORT

http_server = WSGIServer(HOST_PORT, app, log=app.logger)
http_server.serve_forever()
