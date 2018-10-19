from collections import OrderedDict

import pymssql
import pyodbc
import pandas as pd

from serializer import serialize


class Query:
    operators = {
        'Equal': '=',
        'NotEqual': '!=',
        'In': 'IN',
        'NotIn': 'NOT IN',
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

    def executeQuery(self, query, params=None, with_result=True):
        if type(query) != str:
            raise query

        with pyodbc.connect(
                'DRIVER={ODBC Driver 13 for SQL Server};'
                + ('SERVER={server},{port}; DATABASE={database}; \
                    UID={username}; PWD={password}').format(
                    server=self.server, port=1433, database=self.database,
                    username=self.user, password=self.password)
                ) as conn:

            if with_result:
                entries = pd.read_sql_query(query, conn, params=params)
                entries = entries.fillna('').to_dict('records')
                return serialize(entries)
            else:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    row_count = cursor.rowcount
                    return row_count
        return []

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

    def getRequest(self, tablename, s_columns, w_columns=None):
        query = 'SELECT {} FROM {}'
        if s_columns:
            s_columns = ', '.join(s_columns)
        else:
            s_columns = '*'

        args = [s_columns, tablename]

        if w_columns:
            query += ' WHERE {}'
            columns = list()

            for o in w_columns:
                operator = [self.operators[o[0]] for i in range(len(o[1]))]

                if o[0] in ('In', 'NotIn'):
                    cols = ' {} (?) AND '.join(o[1]) + ' {} ?'
                else:
                    cols = ' {} ? AND '.join(o[1]) + ' {} ?'

                cols = cols.format(*operator)
                columns.append(cols)

            columns = ' AND '.join(columns)
            args.append(columns)

        return query.format(*args)

    def deleteRequest(self, tablename, w_columns=None):
        query = 'DELETE FROM {}'
        args = [tablename]

        if w_columns:
            query += ' WHERE {}'
            columns = list()

            for o in w_columns:
                operator = [self.operators[o[0]] for i in range(len(o[1]))]

                if o[0] in ('In', 'NotIn'):
                    cols = ' {} (?) AND '.join(o[1]) + ' {} ?'
                else:
                    cols = ' {} ? AND '.join(o[1]) + ' {} ?'

                cols = cols.format(*operator)
                columns.append(cols)

            columns = ' AND '.join(columns)
            args.append(columns)

        return query.format(*args)

    def postRequest(self, tablename, columns):
        query = 'INSERT INTO {} ({}) VALUES({})'
        values = ', '.join(['?' for column in columns])
        columns = ', '.join(columns)
        args = [tablename, columns, values]

        return query.format(*args)

    def putRequest(self, tablename, u_columns, w_columns=None):
        query = 'UPDATE {} SET {}'
        u_columns = ' = ?, '.join(u_columns) + ' = ?'
        args = [tablename, u_columns]

        if w_columns:
            query += ' WHERE {}'
            columns = list()

            for o in w_columns:
                operator = [self.operators[o[0]] for i in range(len(o[1]))]

                if o[0] in ('In', 'NotIn'):
                    cols = ' {} (?) AND '.join(o[1]) + ' {} ?'
                else:
                    cols = ' {} ? AND '.join(o[1]) + ' {} ?'

                cols = cols.format(*operator)
                columns.append(cols)

            columns = ' AND '.join(columns)
            args.append(columns)

        return query.format(*args)
