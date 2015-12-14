##
# Python Sample Application: Connection to Informix using DRDA
##

# Topics
# 1 Create table 
# 2 Inserts
# 2.1 Insert a single document into a table
# 2.2 Insert multiple documents into a table
# 3 Queries
# 3.1 Find one document in a table that matches a query condition
# 3.2 Find documents in a table that match a query condition
# 3.3 Find all documents in a table
# 3.4 Count documents with query
# 3.5 Order documents in a table
# 3.6 Join tables
# 3.7 Find distinct fields in a table
# 3.8 Find with projection clause
# 4 Update documents in a table
# 5 Delete documents in a table
# 6 Transactions
# 7 Commands
# 7.1 Count
# 7.2 Distinct
# 8 Drop a table

import ibm_db
import json 
import logging
import os
from flask import Flask, render_template

app = Flask(__name__)

# To run locally, set URL and DATABASE information.
# Otherwise, url and database information will be parsed from 
# the Bluemix VCAP_SERVICES.
URL = ""

USE_SSL = False     # Set to True to use SSL url from VCAP_SERVICES
SERVICE_NAME = os.getenv('SERVICE_NAME', 'timeseriesdatabase')
port = int(os.getenv('VCAP_APP_PORT', 8080))

class City:
    def __init__(self, name, population, longitude, latitude, countryCode):
        self.name = name
        self.population = population
        self.longitude = longitude
        self.latitude = latitude
        self.countryCode = countryCode
    def toSQL(self):
        return "('" + self.name + "', " + str(self.population) + ", " + str(self.longitude) + ", " + str(self.latitude) + ", " + str(self.countryCode) + ")"
             
        
kansasCity = City("Kansas City", 467007, 39.0997, 94.5783, 1)
seattle = City("Seattle", 652405, 47.6097, 122.3331, 1)
newYork = City("New York", 8406000, 40.7127, 74.0059, 1)
london = City("London", 8308000, 51.5072, 0.1275, 44)
tokyo = City("Tokyo", 13350000, 35.6833, -139.6833, 81)
madrid = City("Madrid", 3165000, 40.4000, 3.7167, 34)
melbourne = City("Melbourne", 4087000, -37.8136, -144.9631, 61)
sydney = City("Sydney", 4293000, -33.8650, -151.2094, 61)

def getDatabaseInfo():
    """
    Get database url information
    
    :returns: (url)
    """
    
    # Use defaults
    if URL:
        return URL
    
    # Parse database info from VCAP_SERVICES
    if (os.getenv('VCAP_SERVICES') is None):
        raise Exception("VCAP_SERVICES not set in the environment")
    vcap_services = json.loads(os.environ['VCAP_SERVICES'])
    try:
        tsdb = vcap_services[SERVICE_NAME][0]
        credentials = tsdb['credentials']
        database = credentials['db']
        host = credentials['host']
        username = credentials['username']
        password = credentials['password']  
        if USE_SSL:
            port = credentials['drda_port_ssl']
        else:
            port = credentials['drda_port']
        url = "HOSTNAME=" + host + ";PORT=" + str(port) + ";DATABASE="+ database + ";PROTOCOL=TCPIP;UID=" + username +";PWD="+ password + ";"
        if USE_SSL:
            url += "Security=ssl;"
        return url
    
    except KeyError as e:
        logging.error(e)
        raise Exception("Error parsing VCAP_SERVICES. Key " + str(e) + " not found.")
 
    
def doEverything():    
    output = []
    
    # Get database connectivity information
    url = getDatabaseInfo()
    
    # connect to database
    output.append("Connecting to " + url)
    try: 
        conn = ibm_db.connect(url, '', '')
    except: 
        output.append("Could not establish connection to database: " + ibm_db.conn_errormsg())
        return output
    output.append("Connection successful")

    
    # set up variables and data
    tableName = "pythonDRDAGalaxy"
    
    try:
        # 1 Create table
        output.append("\n#1 Create table")
         
        sql = "create table if not exists " + tableName + "(name VARCHAR(255), population INTEGER, longitude DECIMAL(8,4), latitude DECIMAL(8,4),countryCode INTEGER)"
        ibm_db.exec_immediate(conn, sql)
                   
        output.append( "\tCreate a table named: " + tableName)
        output.append("\tCreate Table SQL: " + sql)
        
        # 2 Inserts
        output.append("\n#2 Inserts")
        # 2.1 Insert a single document into a table
        output.append("#2.1 Insert a single document into a table")
        
        sql = "insert into " + tableName + " values(?,?,?,?,?)"
        statement = ibm_db.prepare(conn, sql)
        ibm_db.bind_param(statement, 1, kansasCity.name)
        ibm_db.bind_param(statement, 2, kansasCity.population)
        ibm_db.bind_param(statement, 3, kansasCity.longitude)
        ibm_db.bind_param(statement, 4, kansasCity.latitude)
        ibm_db.bind_param(statement, 5, kansasCity.countryCode)
        ibm_db.execute(statement)
        
        
        output.append("\tCreate Document -> " + kansasCity.name + " , " + str(kansasCity.population) + 
                         " , " + str(kansasCity.longitude) + " , " + str(kansasCity.latitude) + " , " + str(kansasCity.countryCode))
        output.append("\tSingle Insert SQL: " + sql)
        
        # 2.2 Insert multiple documents into a table
        # Currently there is no support for batch inserts with ibm_db
        output.append("#2.2: Insert multiple documents into a table. \n\tCurrently there is no support batch inserts")
        sql = "insert into " + tableName + " values(?,?,?,?,?)"
        statement = ibm_db.prepare(conn, sql)
        ibm_db.bind_param(statement, 1, seattle.name)
        ibm_db.bind_param(statement, 2, seattle.population)
        ibm_db.bind_param(statement, 3, seattle.longitude)
        ibm_db.bind_param(statement, 4, seattle.latitude)
        ibm_db.bind_param(statement, 5, seattle.countryCode)
        ibm_db.execute(statement)
        
        sql = "insert into " + tableName + " values(?,?,?,?,?)"
        statement = ibm_db.prepare(conn, sql)
        ibm_db.bind_param(statement, 1, newYork.name)
        ibm_db.bind_param(statement, 2, newYork.population)
        ibm_db.bind_param(statement, 3, newYork.longitude)
        ibm_db.bind_param(statement, 4, newYork.latitude)
        ibm_db.bind_param(statement, 5, newYork.countryCode)
        ibm_db.execute(statement)
        
        #Alternate way to insert without bindings
        sql = "insert into " + tableName + " values" + tokyo.toSQL()
        ibm_db.exec_immediate(conn, sql)
        
        sql = "insert into " + tableName + " values" + madrid.toSQL()
        ibm_db.exec_immediate(conn, sql)
        
        sql = "insert into " + tableName + " values" + melbourne.toSQL()
        ibm_db.exec_immediate(conn, sql)
        
        # 3 Queries
        output.append("\n#3 Queries")
        
        # 3.1 Find one document in a table that matches a query condition 
        output.append("#3.1 Find one document in a table that matches a query condition")
        
        sql = "select * from " + tableName + " where population > 8000000 and countryCode = 1"
        stmt = ibm_db.exec_immediate(conn, sql)
        dictionary = ibm_db.fetch_both(stmt)
        
        output.append("\tFirst document matching query -> name: " +  str(dictionary[0]) + ", population: " + str(dictionary[1]) + ", longitude: " +
                        str(dictionary[2]) + ", latitude: " + str(dictionary[3]) + ", countryCode: " + str(dictionary[4]))
        output.append("\tQuery By name SQL: " + sql)
        
        # 3.2 Find documents in a table that match a query condition
        output.append("#3.2 Find documents in a table that match a query condition")
        
        sql = "select * from " + tableName + " where population > 8000000 and longitude > 40.0"
        stmt = ibm_db.exec_immediate(conn, sql)
        dictionary = ibm_db.fetch_both(stmt)
        
        output.append("\tFind all documents with name: " + kansasCity.name)
        while dictionary != False:
            output.append("\tFound document name -> name: " +  str(dictionary[0]) + ", population: " + str(dictionary[1]) + ", longitude: " +
                        str(dictionary[2]) + ", latitude: " + str(dictionary[3]) + ", countryCode: " + str(dictionary[4]))
            dictionary = ibm_db.fetch_both(stmt)
        output.append( "\tQuery All By name SQL: " + sql)
        
        # 3.3 Find all documents in a table
        output.append("#3.3 Find all documents in a table")
        
        sql = "select * from " + tableName
        stmt = ibm_db.exec_immediate(conn, sql)
        dictionary = ibm_db.fetch_both(stmt)
        
        output.append( "\tFind all documents in table: " + tableName)
        while dictionary != False:
            output.append("\tFound document name -> name: " +  str(dictionary[0]) + ", population: " + str(dictionary[1]) + ", longitude: " +
                        str(dictionary[2]) + ", latitude: " + str(dictionary[3]) + ", countryCode: " + str(dictionary[4]))
            dictionary = ibm_db.fetch_both(stmt)
        output.append("\tFind All Documents SQL: " + sql)
        
        output.append("#3.4 Count documents in a table")
        sql = "select count(*) from " + tableName + " where longitude < 40.0"
        stmt = ibm_db.exec_immediate(conn, sql)
        dictionary = ibm_db.fetch_both(stmt)
        output.append("Documents in table with longitude less than 40.0: " + str(len(dictionary)))
        
        output.append("#3.5 Order documents in a table")
        sql = "select * from " + tableName + " order by population"
        stmt = ibm_db.exec_immediate(conn, sql)
        dictionary = ibm_db.fetch_both(stmt)
        while dictionary != False:
            output.append("\tFound Document -> name: " + str(dictionary[0]) + " population: " + str(dictionary[1]) + " latitude: " +
                        str(dictionary[2]) + " longitude: " + str(dictionary[3]) + " countryCode: " + str(dictionary[4]))
            dictionary = ibm_db.fetch_both(stmt)
        
        output.append("#3.6 Joins")
        tableJoin = "country";
        sql = "create table if not exists " + tableJoin + " (countryCode INTEGER, countryName VARCHAR(255))";
        stmt = ibm_db.exec_immediate(conn, sql)
        
        sql = "insert into " + tableJoin + " values (1,\"United States of America\")";
        stmt = ibm_db.exec_immediate(conn, sql)
        
        sql = "insert into " + tableJoin + " values (44,\"United Kingdom\")";
        stmt = ibm_db.exec_immediate(conn, sql)
        
        sql = "insert into " + tableJoin + " values (81,\"Japan\")";
        stmt = ibm_db.exec_immediate(conn, sql)
        
        sql = "insert into " + tableJoin + " values (34,\"Spain\")";
        stmt = ibm_db.exec_immediate(conn, sql)
        
        sql = "insert into " + tableJoin + " values (61,\"Australia\")";
        stmt = ibm_db.exec_immediate(conn, sql)
        
        sql = ("select table1.name, table1.population, table1.longitude, table1.latitude, table1.countryCode, table2.countryName from " + 
            tableName + " table1 inner join " + tableJoin + " table2 on table1.countryCode=table2.countryCode")
        stmt =ibm_db.exec_immediate(conn, sql)
        dictionary = ibm_db.fetch_both(stmt)
        while dictionary != False:
            output.append("\tJoined -> name: " + str(dictionary[0]) + ", population: " + str(dictionary[1]) + ", latitude: " +
                        str(dictionary[2]) + ", longitude: " + str(dictionary[3]) + ", countryCode: " + str(dictionary[4]) + ", countryName: " + str(dictionary[5]))
            dictionary = ibm_db.fetch_both(stmt)
        
        output.append("#3.7 Distinct documents in a table")
        sql = "select distinct countryCode from " + tableName + " where longitude > 40.0"
        stmt = ibm_db.exec_immediate(conn, sql)
        dictionary = ibm_db.fetch_both(stmt)
        output.append("Distinct countryCodes of documents in table with longitude greater than 40.0: ")
        while dictionary != False:
            output.append("\tJoined -> countryCode: " + str(dictionary[0]))
            dictionary = ibm_db.fetch_both(stmt)
            
        output.append("#3.8 Projection Clause")
        sql = "select distinct name, countryCode from " + tableName + " where population > 8000000"
        stmt = ibm_db.exec_immediate(conn, sql)
        dictionary = ibm_db.fetch_both(stmt)
        output.append("Projection of name and countryCode where population is greater than 8000000: ")
        while dictionary != False:
            output.append("\tJoined -> name: " + str(dictionary[0]) + ", countryCode: " + str(dictionary[1]))
            dictionary = ibm_db.fetch_both(stmt)
        
        # 4 Update documents in a table
        output.append("\n#4 Update documents in a table")
        
        sql = "update " + tableName + " set countryCode = ? where name = ?"
        statement = ibm_db.prepare(conn, sql)
        ibm_db.bind_param(statement, 1, 999)
        ibm_db.bind_param(statement, 2, seattle.name)
        ibm_db.execute(statement)
        
        output.append( "\tDocument to update: " + seattle.name)
        output.append("\tUpdate By name SQL: " + sql)
        
        
        # 5 Delete documents in a table
        output.append("\n#5 Delete documents in a table")
        
        sql = "delete from " + tableName + " where name like '" + newYork.name + "'"
        ibm_db.exec_immediate(conn, sql)
        
        output.append("\tDelete documents with name: " + newYork.name)
        output.append("\tDelete By name SQL: " + sql)
        
        output.append("\n#6 Transactions")
    #     ibm_db.autocommit(conn, False)
    #     
    #     sql = "insert into " + tableName + " values" + sydney.toSQL()
    #     ibm_db.exec_immediate(conn, sql)
    #     
    #     ibm_db.commit(conn)
    #     
    #     sql = "update " + tableName + " set countryCode = 998 where name = 'Seattle'"
    #     ibm_db.exec_immediate(conn, sql)
    #     
    #     ibm_db.rollback(conn)
    #     
    #     ibm_db.autocommit(conn, True)
        
        
        output.append( "\tFind all documents in table: " + tableName)
        sql = "select * from " + tableName
        stmt = ibm_db.exec_immediate(conn, sql)
        dictionary = ibm_db.fetch_both(stmt)
        while dictionary != False:
            output.append("\tFound document name -> name: " +  str(dictionary[0]) + ", population: " + str(dictionary[1]) + ", longitude: " +
                        str(dictionary[2]) + ", latitude: " + str(dictionary[3]) + ", countryCode: " + str(dictionary[4]))
            dictionary = ibm_db.fetch_both(stmt)
        
        output.append("\n#7 Commands")
        output.append("\n#7.1 Count")
        sql = "select count(*) from " + tableName
        stmt = ibm_db.exec_immediate(conn, sql)
        dictionary = ibm_db.fetch_both(stmt)
        output.append("Number of documents in table " + str(len(dictionary)))
        
        # 8 Drop a table
        output.append("\n#8 Drop a table")
        
        sql = "drop table " + tableName;
        ibm_db.exec_immediate(conn, sql)
        
        sql = "drop table " + tableJoin;
        ibm_db.exec_immediate(conn, sql)
        
        output.append("\tDrop table: " + tableName)
        output.append("\tDrop Table SQL: " + sql)
    
    except Exception as e:
        logging.exception(e) 
        output.append("EXCEPTION (see log for details): " + str(e))
    finally:
        if conn:
            ibm_db.close(conn)
            output.append("\nConnection closed")
            
    return output

@app.route("/")
def displayPage():
    return render_template('index.html')

@app.route("/databasetest")
def runSample():
    output = []
    try:
        output = doEverything()
    except Exception as e:
        logging.exception(e) 
        output.append("EXCEPTION (see log for details): " + str(e))
    return render_template('tests.html', output=output)

if (__name__ == "__main__"):
    app.run(host='0.0.0.0',port=port)
