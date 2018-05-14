# Change this in deployment
activate_this = '~/Desktop/env/quart/bin/activate_this.py'

with open(activate_this) as file_:
    exec(file_.read(), dict(__file__=activate_this))

from cgc_api import app as application