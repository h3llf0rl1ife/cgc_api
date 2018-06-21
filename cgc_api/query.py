from datetime import datetime
from decimal import Decimal
from collections import OrderedDict

import pymssql


class Query:
    operators = {
        'Equal': '=',
        'NotEqual': '!=',
        'GreaterThan': '>',
        'GreaterEqual': '>=',
        'LessThan': '<',
        'LessEqual': '<=',
        'Like': 'LIKE',
        'ILike': 'ILIKE',
        'NotLike': 'NOT LIKE',
        'NotILike': 'NOT ILIKE'
    }

    def __init__(self, server, user, password, database):
        self.server = server
        self.user = user
        self.password = password
        self.database = database
        self.connParams = (self.server, self.user,
                           self.password, self.database)

    def executeQuery(self, query, param=None, with_result=True):
        if type(query) != str:
            raise query

        with pymssql.connect(*self.connParams) as conn:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute(query, param)
                row_count = cursor.rowcount

                if with_result is not True:
                    conn.commit()

                if with_result is True:
                    entries = cursor.fetchall()
                    for entry in entries:
                        for cell in entry:
                            if type(entry[cell]) is datetime:
                                entry[cell] = str(entry[cell])
                            elif type(entry[cell]) is Decimal:
                                entry[cell] = float(entry[cell])
                    return entries
        return row_count

    def validateTable(self, table):
        with pymssql.connect(*self.connParams) as conn:
            with conn.cursor(as_dict=True) as cursor:
                try:
                    query = 'SELECT name FROM {}.sys.Tables'.format(
                        self.database)
                    cursor.execute(query)
                except pymssql.ProgrammingError:
                    raise

                for row in cursor:
                    if table == row['name']:
                        return row['name']

                raise KeyError

    def validateColumn(self, table, column, is_list=False, is_dict=False):
        with pymssql.connect(*self.connParams) as conn:
            with conn.cursor(as_dict=True) as cursor:
                try:
                    query = '''SELECT name FROM {}.sys.columns
                               WHERE object_id = OBJECT_ID(\'{}\')
                            '''.format(self.database, table)

                    cursor.execute(query)
                except pymssql.ProgrammingError:
                    raise

                if is_list:
                    columns = column
                    new_columns = []
                    for row in cursor:
                        for column in columns:
                            if column == row['name']:
                                new_columns.append(row['name'])
                    return new_columns
                elif is_dict:
                    columns = column
                    new_columns = OrderedDict(column)
                    rows = cursor.fetchall()

                    for operator in columns:
                        cols = list()
                        for row in rows:
                            for col in columns[operator]:
                                if col == row['name']:
                                    cols.append(col)

                        new_columns[operator] = cols
                    return new_columns

                else:
                    for row in cursor:
                        if column == row['name']:
                            return row['name']
                return None

    def getRequest(self, tablename, column=None):
        query = 'SELECT * FROM {}'
        args = [tablename]

        if column is not None:
            query += ' WHERE {} = %s'
            args.append(column)

        return query.format(*args)

    def deleteRequest(self, tablename, columns=None):
        query = 'DELETE FROM {}'
        args = [tablename]

        if columns is not None:
            query += ' WHERE {}'
            columns = ' = %s AND '.join(columns) + ' = %s'
            args.append(columns)

        return query.format(*args)

    def postRequest(self, tablename, columns):
        query = 'INSERT INTO {} ({}) VALUES({})'
        values = ', '.join(['%s' for column in columns])
        columns = ', '.join(columns)
        args = [tablename, columns, values]

        return query.format(*args)

    def putRequest(self, tablename, u_columns, w_columns=None, **kwargs):
        query = 'UPDATE {} SET {} WHERE {}'
        u_columns = ' = %s, '.join(u_columns) + ' = %s'

        if w_columns:
            w_columns = ' = %s AND '.join(w_columns) + ' = %s'
            columns = w_columns

        elif kwargs:
            columns = set()
            for kwarg in kwargs:
                operator = self.operators[kwarg]
                cols = [*kwargs[kwarg]]
                cols = ' {} %s AND '.join(cols) + ' {} %s'
                columns.add(cols.format(operator))

            columns = ' AND '.join(columns)

        args = [tablename, u_columns, columns]

        return query.format(*args)

    # TODO: Add support for multiple values for the same column
