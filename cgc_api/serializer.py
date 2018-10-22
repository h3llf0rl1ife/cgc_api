import json
import pandas as pd
import numpy
import datetime

# import decimal


class ComplexEncoder(json.JSONEncoder):
    def default(self, z):
        if isinstance(z, pd._libs.tslibs.timestamps.Timestamp):
            return z.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(z, datetime.datetime):
            return z.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(z, datetime.date):
            return z.strftime('%Y-%m-%d')
        elif isinstance(z, numpy.int64):
            return z.item()

        else:
            super().default(self, z)


def serialize(data):
    return json.loads(data)
