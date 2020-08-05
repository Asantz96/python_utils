#CONNECTOR TO MySQL DATABASE
#AUTHOR: ANDRÉS SANTOS 

#The MySQL package is imported

import mysql.connector
from mysql.connector import Error

def MySQLConnect(usernameStr, passwordStr, hostStr, databaseStr, conPrint = True):
    #ESTABLISH THE CONNECTION WITH THE DATABASE
    try:
        con = mysql.connector.connect(
            user = usernameStr,
            password = passwordStr,
            host = hostStr,
            database = databaseStr)

        if conPrint: print('Connection established successfully with the database')
        return con
    except Error:
        print(Error)


def MySQLSelectQuery(con, strQuery):
    #USE FOR SELECT STATEMENT QUERY AND STORE IT IN A VARIABLE
    csr = con.cursor()
    try: 
        csr.executemany(strQuery)
        queryResult= csr.fetchall() #USED TO STORE IN A VARIABLE THE QUERY
        return queryResult
            # [print(row) for row in cursorObj.fetchall()] #PRINT QUERY RESULTS
    except Error:
        print(Error)

def MySQLGeneralQuery(con, strQuery): 
    #USE FOR CREATE AND DROP TABLE, INSERT OR UPDATE 
    csr = con.cursor()
    try: 
        csr.execute(strQuery) 
        con.commit()
    except  Error:
        print(Error)

def MySQLInsertMultiple(con, strQuery, data):
    #BULK INSERT 
    csr = con.cursor()
    try:
        csr.execute(strQuery, data) 
        con.commit()
    except  Error:
        print(Error)

def MySQLCloseConnection(con, conPrint= True):
     #CLOSE THE CONNECTION WITH THE DATABASE
    try:
        con.close()
        if conPrint: print('Connection closed successfully with the database')
    except  Error:
        print(Error)
