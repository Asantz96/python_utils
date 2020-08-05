#CONNECTOR TO SQLite DATABASE
#AUTHOR: ANDRÉS SANTOS 

#The SQLite3 package is imported

import sqlite3
from sqlite3 import Error


def SQLiteConnect(root, file, conPrint = True):
    #ESTABLISH THE CONNECTION WITH THE DATABASE
    try:
        con = sqlite3.connect(root +  '/' + file)
        if conPrint: print('Connection established successfully with the database')
        return con
    except Error:
        print(Error)


def SQLiteSelectQuery(con, strQuery):
    #USE FOR SELECT STATEMENT QUERY AND STORE IT IN A VARIABLE
    csr = con.cursor()
    try: 
        csr.execute(strQuery)
        queryResult= csr.fetchall() #USED TO STORE IN A VARIABLE THE QUERY
        return queryResult
            # [print(row) for row in cursorObj.fetchall()] #PRINT QUERY RESULTS
    except Error:
        print(Error)

def SQLiteGeneralQuery(con, strQuery): 
    #USE FOR CREATE AND DROP TABLE, INSERT OR UPDATE 
    csr = con.cursor()
    try: 
        csr.execute(strQuery) 
        con.commit()
    except Error:
        print(Error)

def SQLiteInsertMultiple(con, strQuery, data):
    #BULK INSERT 
    csr = con.cursor()
    try:
        csr.executemany(strQuery, data) 
        con.commit()
    except Error:
        print(Error)

def SQLiteCloseConnection(con):
     #CLOSE THE CONNECTION WITH THE DATABASE
    try:
        con.close()
        print('Connection closed successfully with the database')
    except Error:
        print(Error)
