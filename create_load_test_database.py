import cassandra
import csv

# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()

# Create a Keyspace name 'sparkify'

try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS sparkify 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)
	
# Set KEYSPACE to the keyspace 'sparkify'

try:
    session.set_keyspace('sparkify')
except Exception as e:
    print(e)
	
## Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4
## 
## CREATE TABLE EXECUTION

query = """CREATE TABLE IF NOT EXISTS session_library (
           sessionID INT
         , itemInSession INT
         , artist TEXT
         , song TEXT
         , length DECIMAL
         , PRIMARY KEY (sessionID, itemInSession)
           )"""
try:
    session.execute(query)
except Exception as e:
    print(e) 
	
## INSERT DATA INTO TABLES

file = 'event_datafile_new.csv' # event datafile generated from event_data files

with open(file, encoding = 'utf8') as f: # opens event datafile
    csvreader = csv.reader(f) # generates csv object
    next(csvreader) # skips header row
    
    for line in csvreader: # iterates through every row in the event datafile
        
        ## Generates a row insertion statement, injecting applicable values from the event datafile
        query = "INSERT INTO session_library (sessionID, itemInSession, artist, song, length) "
        query = query + "VALUES (%s, %s, %s, %s, %s)"
        
        ## All values in the event datafile are stored as strings, thus some values must be converted to the appropriate
        ## datatypes as dictated by the table definition. Then the query is executed for each row in the event datafile.
        session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))
		
## EXECUTE TEST SELECT QUERY

query = """SELECT artist, song, length
           FROM session_library
           WHERE sessionID = 338 AND itemInSession = 4;"""

results = session.execute_async(query) # execute_async allows for generation of result() object for printing
rows = results.result()

## print all rows of result table
print("QUERY 1 TEST:")
for row in rows:
    print(row)
	
## Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182
## 
## CREATE TABLE EXECUTION

query = """CREATE TABLE IF NOT EXISTS user_library (
           userID INT
         , sessionID INT
         , itemInSession INT
         , song TEXT
         , artist TEXT
         , firstName TEXT
         , lastName TEXT
         , PRIMARY KEY (userID, sessionID, itemInSession)
           )"""
try:
    session.execute(query)
except Exception as e:
    print(e)   
	
## INSERT DATA INTO TABLE

file = 'event_datafile_new.csv' # event datafile generated from event_data files

with open(file, encoding = 'utf8') as f: # opens event datafile
    csvreader = csv.reader(f) # generates csv object
    next(csvreader) # skips header row
    
    for line in csvreader: # iterates through every row in the event datafile
        
        ## Generates a row insertion statement, injecting applicable values from the event datafile
        query = "INSERT INTO user_library (userID, sessionID, itemInSession, artist, song, firstName, lastName)"
        query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        
        ## All values in the event datafile are stored as strings, thus some values must be converted to the appropriate
        ## datatypes as dictated by the table definition. Then the query is executed for each row in the event datafile.
        session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))  
		
## EXECUTE TEST SELECT QUERY

query = """SELECT artist, song, firstName, lastName
           FROM user_library
           WHERE userID = 10 AND sessionID = 182;"""

results = session.execute_async(query) # execute_async allows for generation of result() object for printing
rows = results.result()

## print all rows of result table
print("QUERY 2 TEST:")
for row in rows:
    print(row)
	
## Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
## 
## CREATE TABLE EXECUTION

query = """CREATE TABLE IF NOT EXISTS song_library (
           song TEXT
         , artist TEXT
         , userID INT
         , firstName TEXT
         , lastName TEXT
         , PRIMARY KEY (song, artist, userID)
           )"""
try:
    session.execute(query)
except Exception as e:
    print(e) 
	
## INSERT DATA INTO TABLE

file = 'event_datafile_new.csv' # event datafile generated from event_data files

with open(file, encoding = 'utf8') as f: # opens event datafile
    csvreader = csv.reader(f) # generates csv object
    next(csvreader) # skips header row
    
    for line in csvreader: # iterates through every row in the event datafile
        
        ## Generates a row insertion statement, injecting applicable values from the event datafile
        query = "INSERT INTO song_library (song, artist, userID, firstName, lastName)"
        query = query + "VALUES (%s, %s, %s, %s, %s)"
        
        ## All values in the event datafile are stored as strings, thus some values must be converted to the appropriate
        ## datatypes as dictated by the table definition. Then the query is executed for each row in the event datafile
        session.execute(query, (line[9], line[0], int(line[10]), line[1], line[4]))  
		
## EXECUTE TEST SELECT QUERY

query = """SELECT firstName, lastName
           FROM song_library
           WHERE song = 'All Hands Against His Own';"""

results = session.execute_async(query) # execute_async allows for generation of result() object for printing
rows = results.result()

## print all rows of result table
print("QUERY 3 TEST:")
for row in rows:
    print(row)
	

## drop tables	
# session.execute("DROP TABLE IF EXISTS session_library;")
# session.execute("DROP TABLE IF EXISTS user_library;")
# session.execute("DROP TABLE IF EXISTS song_library;")

## shutdown session and cluster
session.shutdown()
cluster.shutdown()