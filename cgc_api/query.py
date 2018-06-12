from datetime import datetime
from decimal import Decimal
import pymssql


class Query:
    def __init__(self, server, user, password, database):
        self.server = server
        self.user = user
        self.password = password
        self.database = database
        self.connParams = (self.server, self.user, self.password, self.database)
    
    
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
                    query = 'SELECT name FROM {}.sys.Tables'.format(self.database)
                    cursor.execute(query)
                except pymssql.ProgrammingError:
                    raise
                
                for row in cursor:
                    if table == row['name']:
                        return row['name']

                raise KeyError
    

    def validateColumn(self, table, column):
        with pymssql.connect(*self.connParams) as conn:
            with conn.cursor(as_dict=True) as cursor:
                try:
                    query = 'SELECT name FROM {}.sys.columns WHERE object_id = OBJECT_ID(\'{}\')'.format(self.database, table)
                    cursor.execute(query)
                except pymssql.ProgrammingError:
                    raise
                
                for row in cursor:
                    if column == row['name']:
                        return row['name']
                return None
    
    
    def getRequest(self, tablename, column=None):
        query = 'SELECT * FROM [{}]'
        args = [tablename]

        if column is not None:
            query += ' WHERE [{}] = %s'
            args.append(column)
        
        return query.format(*args)


    def deleteRequest(self, tablename, column=None):
        query = 'DELETE FROM {}'
        args = [tablename]

        if column is not None:
            query += ' WHERE [{}] = %s'
            args.append(column)

        return query.format(*args)
