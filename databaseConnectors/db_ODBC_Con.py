#CONNECTOR TO A DATABASE USING A ODBC DRIVER
#AUTHOR: ANDRÉS SANTOS 

#The ODBC driver package is imported
# THE ODBC DRIVER COULD BE USE FOR WORK WITH 
# - Google BigQuery
# - Microsoft Access
# - Microsoft Excel
# - MySQL
# - Netezza
# - PostgreSQL
# - SQLite
# - Teradata
# - Vertica

import pyodbc


def ODBCConnect(cString, conPrint = True):
    #ESTABLISH THE CONNECTION WITH THE DATABASE
    try:
        con = pyodbc.connect(cString)
        if conPrint: print('Connection established successfully with the database')
        return con
    except pyodbc.Error as ex:
        sqlstate = ex.args[1]
        print(sqlstate)


def ODBCSelectQuery(con, strQuery):
    #USE FOR SELECT STATEMENT QUERY AND STORE IT IN A VARIABLE
    csr = con.cursor()
    try: 
        csr.execute(strQuery)
        queryResult= csr.fetchall() #USED TO STORE IN A VARIABLE THE QUERY
        return queryResult
            # [print(row) for row in cursorObj.fetchall()] #PRINT QUERY RESULTS
    except pyodbc.Error as ex:
        sqlstate = ex.args[1]
        print(sqlstate)

def ODBCGeneralQuery(con, strQuery): 
    #USE FOR CREATE AND DROP TABLE, INSERT OR UPDATE 
    csr = con.cursor()
    try: 
        csr.execute(strQuery) 
        con.commit()
    except pyodbc.Error as ex:
        sqlstate = ex.args[1]
        print(sqlstate)

def ODBCInsertMultiple(con, strQuery, data):
    #BULK INSERT 
    csr = con.cursor()
    try:
        csr.executemany(strQuery, data) 
        con.commit()
    except pyodbc.Error as ex:
        sqlstate = ex.args[1]
        print(sqlstate)

def ODBCCloseConnection(con, conPrint = True):
     #CLOSE THE CONNECTION WITH THE DATABASE
    try:
        con.close()
        if conPrint: print('Connection closed successfully with the database')
    except pyodbc.Error as ex:
        sqlstate = ex.args[1]
        print(sqlstate)
