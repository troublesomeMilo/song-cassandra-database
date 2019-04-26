## Data Modeling with Apache Cassandra - Song Play Database

By Harley Hutchins

### Objective

The objective of this project is to demostrate use of Apache Cassandra to build a noSQL database and to develop a simple ETL pipeline to populate the database with song play data pulled from CSV files. With Cassandra being a noSQL database system, the tables must be denormalized and tailored to the queries that will be run against the cluster. The following queries are representative of those that will be regular run against the database by analysts:

1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

### To Run the Program

### Inputs

The input dataset for this project comes in the form of CSV files logging song play data. The columns in the files include, among others, the following that are useful to the analysts:

- artist
- firstName
- gender
- itemInSession
- lastName
- length
- level
- location
- sessionId
- song
- userId 

There is a unique event data file for each day, spanning a month for the purpose of this project. The files are stored in a local directory `/event_data`.

### Methodology

#### Data Processing

Data is stored is multiple log files with numerous columns, many of which are of no use to the analysts. To facilitate easy loading, the data files are iterated through, useful columns extracted, and loaded into a single file that can be used for populating the database. The data processing is performed in python.

First, a number of pertinent libraries are imported:

    import pandas as pd
    import cassandra
    import re
    import os
    import glob
    import numpy as np
    import json
    import csv

The filepath to the data files is determined. The the directory is walked through, creating a list of the filepaths of all the event data files:

    filepath = os.getcwd() + '/event_data'
    for root, dirs, files in os.walk(filepath):
        file_path_list = glob.glob(os.path.join(root,'*'))

The event data files are iterated through, adding each row of raw data to a large, single list:

    full_data_rows_list = []
    for f in file_path_list:
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            csvreader = csv.reader(csvfile) 
            next(csvreader)
            for line in csvreader:
                full_data_rows_list.append(line) 

To facilitate easier loading of the database, the raw data list is cut down to only the columns of use to the analysts and loaded into a single csv file:

    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                         'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

Now the data is prepped for loading into the database tables in the form of a single csv file `event_datafile_new.csv`.

#### Schema

The queries presented in the Objective section dictate the schema and number of tables needed for the database. Cassandra includes a query language very similar to SQL. Thus all the Cassandra queries closely resemble SQL queries and should be fairly easy to understand if you have experience with SQL databases.

1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

This query implies the need for a primary key based on session ID and item in sesssion given that they are mentioned explicitly in the example query. Since the item in session values are unique to each session ID, the table should be partitioned by session ID. Item in session should be a clustering column. These two columsn should be sufficient to uniquely identify each row. Additionally the columns asked for should be included in the table.

**session_library**
- sessionId: partition key
- itemInSession: clustering key
- artist
- song
- length

**QUERY**
    
    SELECT artist, song, length
    FROM session_library
    WHERE sessionID = 338 AND itemInSession = 4;

2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182 

This query implies the need for a primary key based on user ID and session ID given that they are mentioned explicitly in the example query. There is also a need to sort by item in session because songs are uniquely identified in a session by the item in session ID. Thus the partition key should be user ID. This will make each partition unique to each user, creating a log of songs played, ordered by session ID and item in session. The session ID and item in session should be the clustering columns. These columns are sufficient to uniquely identify each row. Additionally the columns asked for should be included in the table.

**user_library**
- userId: partition key
- sessionId: clustering key
- itemIdSession: clustering key
- song
- artist
- firstName
- lastName

**QUERY**

    SELECT artist, song, firstName, lastName
    FROM user_library
    WHERE userID = 10 AND sessionID = 182;

3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

This query implies the need for a primary key based on song given that a song name is mentioned explicitly in the example query. Since no other conditional columns are mentioned in the query, song name should act as the partition key. This will create partitions unique to each song, logging the users that play that given song. Adding clustering columns of artist and userId should create a unique primary key for each row in the table.

**song_library**
- song: partition key
- artist: clustering key
- userId: clustering key
- firstName
- lastName

**QUERY**

    SELECT firstName, lastName
    FROM song_library
    WHERE song = 'All Hands Against His Own';

#### Creating the Tables

The Cassandra driver module `cassandra` is used to interact with the database. First a local cluster is connected to:

    from cassandra.cluster import Cluster
    cluster = Cluster()
    session = cluster.connect()

Keyspace `sparkify` is created:

    try:
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS sparkify 
        WITH REPLICATION = 
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
    )
    except Exception as e:
        print(e)

The keyspace is set so that queries can be executed in it:

    try:
        session.set_keyspace('sparkify')
    except Exception as e:
        print(e)

##### QUERY 1

The table for the first query is created:

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

The data is extracted from the event datafile and loaded into the table:

    file = 'event_datafile_new.csv' 
    with open(file, encoding = 'utf8') as f: 
        csvreader = csv.reader(f) 
        next(csvreader)    
        for line in csvreader: 
            query = "INSERT INTO session_library (sessionID, itemInSession, artist, song, length) "
            query = query + "VALUES (%s, %s, %s, %s, %s)"
            session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))

The example query is run against the table to validate the code:

    query = """SELECT artist, song, length
               FROM session_library
               WHERE sessionID = 338 AND itemInSession = 4;"""
    results = session.execute_async(query)
    rows = results.result()
    print("QUERY 1 TEST:")
    for row in rows:
        print(row)

##### QUERY 2

The Table for the second query is created:

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

The data is extracted from the event datafile and loaded into the table:

    file = 'event_datafile_new.csv' 
    with open(file, encoding = 'utf8') as f: 
        csvreader = csv.reader(f) 
        next(csvreader) 
        for line in csvreader: 
            query = "INSERT INTO user_library (userID, sessionID, itemInSession, artist, song, firstName, lastName)"
            query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
            session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))

The example query is run against the table to validate the code:

    query = """SELECT artist, song, firstName, lastName
               FROM user_library
               WHERE userID = 10 AND sessionID = 182;"""
    results = session.execute_async(query)
    rows = results.result()
    print("QUERY 2 TEST:")
    for row in rows:
        print(row)

##### QUERY 3

The Table for the third query is created:

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

The data is extracted from the event datafile and loaded into the table:

file = 'event_datafile_new.csv' # event datafile generated from event_data files

    with open(file, encoding = 'utf8') as f: 
        csvreader = csv.reader(f) 
        next(csvreader) 
        for line in csvreader: 
            query = "INSERT INTO song_library (song, artist, userID, firstName, lastName)"
            query = query + "VALUES (%s, %s, %s, %s, %s)"
            session.execute(query, (line[9], line[0], int(line[10]), line[1], line[4]))

The example query is run against the table to validate the code:

    query = """SELECT firstName, lastName
               FROM song_library
               WHERE song = 'All Hands Against His Own';"""
    results = session.execute_async(query) 
    rows = results.result()
    print("QUERY 3 TEST:")
    for row in rows:
        print(row)

### Dropping Tables and Shutting down the connection/cluster

    session.execute("DROP TABLE IF EXISTS session_library;")
    session.execute("DROP TABLE IF EXISTS user_library;")
    session.execute("DROP TABLE IF EXISTS song_library;")
    session.shutdown()
    cluster.shutdown()
 
