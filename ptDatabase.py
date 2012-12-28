#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
    Python Library for reading and writing 'Gnip stuff' to a MySQL database.
    Written to provide a simple example of writing PowerTrack rules and activities
    to a local database.

    Based on the standard MySQL Connector/Python driver.
    http://dev.mysql.com/doc/connector-python/en/index.html

    Was planning on using the old MySQLdb library because I had so much legacy code based on it.
    However, getting that installed on Mac 10.8 64-bit looked like a long, slow road full of potholes.

    So, onward!
'''
__title__ = 'ptDatabase'
__version__ = '0.0.1'
__author__ = 'Jim Moffitt'
__license__ = 'Apache 2.0'
__copyright__ = 'Copyright 2013 Gnip'

import mysql.connector
import logging
from pprint import pprint

#--------------------------------------------
logger = logging.getLogger()


'''
class dbLogger:
    def getLogger(parentLog):
        global logger
        logger = parentLog
        logger.debug('Common Gnip database module loading shared logger...')

class dbConfigParser:
    def __init__(self,parser):
        self.parser = parser
'''



#--------------------------------------------
'''
    Helper class for returning connector payload as Dictionary.
'''
class MySQLCursorDict(mysql.connector.cursor.MySQLCursor):
    def _row_to_python(self, rowdata, desc=None):
        row = super(MySQLCursorDict, self)._row_to_python(rowdata, desc)
        if row:
            return dict(zip(self.column_names, row))
        return None


'''
    Main Application-level database class.  Currently intended to be application Singleton.

    Maintains a single database connection.

    Database connection details are passed in from Application Config file.
    Database system messages are written to 'log_db' file, as specified in Application
    Config file.

'''
class ptDatabase():

    def __init__(self,host,port,database,user,password):
        self.dbConfig = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }

        #logger.debug('Creating ptDatabase object looking at ' + self.dbConfig['host'] + '...')

    def runSELECT(self,query):
        try:
            cnx = mysql.connector.connect(**self.dbConfig) #Make database connection.
            cursor = cnx.cursor(cursor_class=MySQLCursorDict) #Create cursor that returns a Dictionary.
            cursor.execute(query) #Execute query.
            rowsDict = cursor.fetchall()  #Load results into dictionary.
            #Close cursor and database.
            cursor.close()
            cnx.close()
        except mysql.connector.Error as err:
            #TODO: error handling.
            logger.err('Database error: ' + err)
            rowsDict = {}

        return rowsDict

    def runINSERT(self,query):
        try:
            cnx = mysql.connector.connect(**self.dbConfig) #Make database connection.
            cursor = cnx.cursor() #Create a default cursor.
            cursor.execute(query) #Execute query.
            cnx.commit()  #Commit to database.

            #Close cursor and database.
            cursor.close()
            cnx.close()
        except mysql.connector.Error as err:
            #TODO: error handling.
            pass
            #logger.err('Database error: ' + err)

    def runSQL(self, query):
        try:
            #If SELECT statement.
            if 'SELECT' in query.upper():
                self.runSELECT(self,query)

            if 'INSERT' in query.upper():
                self.runINSERT(self,query)

        except:
            pass
            #logger.error('Database error in runSQL..')

    def getRules(self):
        query = 'SELECT * FROM rules;'
        rulesDict = {}
        rulesDict = self.runSQL(query)

        return rulesDict

    def setRules(self,dictRules):
        pass

    def addRule(self,value):
        query = "INSERT INTO rules (rule,create_time, modified_time) VALUES ('" + value + "',NOW(), NOW());"
        self.runSQL(query)

    def updateRules(self,dictRules):
        pass

    def deleteRules(self):
        pass

    def getActivities(self, LIMIT):
        query = 'SELECT * FROM activities;'
        actDict = {}
        actDict = self.runSQL(query)
        return actDict

    def addActivity(self,activity):
        pass

'''
class MySQLCursorDict(mysql.connector.cursor.MySQLCursor):
    def _row_to_python(self, rowdata, desc=None):
        row = super(MySQLCursorDict, self)._row_to_python(rowdata, desc)
        if row:
            return dict(zip(self.column_names, row))
        return None
         
cnx = mysql.connector.connect(user='root', database='test')
cur = cnx.cursor(cursor_class=MySQLCursorDict)
cur.execute("SELECT c1, c2 FROM t1")
rows = cur.fetchall()
pprint(rows)
cur.close()
cnx.close()
'''


'''
Unit testing.
'''
#print 'Testing ptDB...'
#ptDB = ptDatabase('127.0.0.1',3306,'gnip_data','root','')
#ptDB.addRule('snow warming climate')




