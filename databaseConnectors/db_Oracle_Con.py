#CONNECTOR TO Oracle DATABASE
#AUTHOR: ANDRÉS SANTOS 

#The Oracle package is imported

import cx_Oracle

def OracleConnect(username, password, dsn, encodingType = 'UTF-8', conPrint = True):
    #ESTABLISH THE CONNECTION WITH THE DATABASE
    try:
        con = cx_Oracle.connect(
            username,
            password,
            dsn,
            encodingType)

        if conPrint: print('Connection established successfully with the database')
        return con
    except cx_Oracle.Error as Error:
        print(Error)

def OracleSessionPool(username, password, dsn, defMin, defMax, isThreaded = True ,encodingType = 'UTF-8', conPrint = True):
    #ESTABLISH THE CONNECTION POOL WITH THE DATABASE
    try:
        con = cx_Oracle.SessionPool( username, password,  dsn, encodingType,  min = defMin,  max = defMax, threaded = isThreaded)

        if conPrint: print('Session pool established successfully with the database')
        return con
    except cx_Oracle.Error as Error:
        print(Error)

def OracleSelectQuery(con, strQuery):
    #USE FOR SELECT STATEMENT QUERY AND STORE IT IN A VARIABLE
    csr = con.cursor()
    try: 
        csr.executemany(strQuery)
        queryResult= csr.fetchall() #USED TO STORE IN A VARIABLE THE QUERY
        return queryResult
            # [print(row) for row in cursorObj.fetchall()] #PRINT QUERY RESULTS
    except cx_Oracle.Error as Error:
        print(Error)

def OracleGeneralQuery(con, strQuery): 
    #USE FOR CREATE AND DROP TABLE, INSERT OR UPDATE 
    csr = con.cursor()
    try: 
        csr.execute(strQuery) 
        con.commit()
    except cx_Oracle.Error as Error:
        print(Error)

def OracleInsertMultiple(con, strQuery, data):
    #BULK INSERT 
    csr = con.cursor()
    try:
        csr.execute(strQuery, data) 
        con.commit()
    except cx_Oracle.Error as Error:
        print(Error)

def OracleCloseConnection(con, conPrint = True):
     #CLOSE THE CONNECTION WITH THE DATABASE
    try:
        con.close()
        if conPrint: print('Connection closed successfully with the database')
    except cx_Oracle.Error as Error:
        print(Error)
