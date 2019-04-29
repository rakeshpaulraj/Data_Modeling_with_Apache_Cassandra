
# Import Python packages
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from sql_queries import *
from cassandra.cluster import Cluster

def create_keyspace():
    ''' 
    Function to Connect to Cassandra cluster and create keyspace

    Parameters: None
  
    Returns   :   
    cluster   : Cassandra cluster
    session   : Cassandra session      
    '''
    # Create Cassandra Cluster and make a connection to a Cassandra instance in local machine
    try:
        # Create Cassandra Cluster
        cluster = Cluster(['127.0.0.1'])
        # To establish connection and begin executing queries, need a session
        session = cluster.connect()
    except Exception as e:
        print(e)

    # Create a Keyspace
    try:
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS sparkify 
        WITH REPLICATION = 
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
                        )
    except Exception as e:
        print(e)

    # Set KEYSPACE to the keyspace specified above
    try:
        session.set_keyspace('sparkify')
    except Exception as e:
        print(e)

    return cluster, session


def drop_tables(cluster, session):
    ''' 
    Function to drop the Cassandra tables if they exist already

    Parameters: 
    cluster   : Cassandra cluster
    session   : Cassandra session 
  
    Returns   : None      
    '''
    for query in drop_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)

def create_tables(cluster, session):
    ''' 
    Function to create the Cassandra tables.

    Parameters: 
    cluster   : Cassandra cluster
    session   : Cassandra session 
  
    Returns   : None      
    '''
    for query in create_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)

def execute_select_query(cluster, session, query):
    ''' 
    Function to execute a select query in Cassandra database.

    Parameters: 
    cluster   : Cassandra cluster
    session   : Cassandra session 
    query     : Query string to be executed
  
    Returns   : rows returned by query      
    '''
    try:
        print("---------------------------------------------------------------------------------------")
        print("Execute query.. '" + query + "'")
        print("---------------------------------------------------------------------------------------")
        rows = session.execute(query)
    except Exception as e:
        print(e)

    return rows


def main():
    ''' 
    Thsi script/function performs the Creation, Modeling and Loading of Apache Cassandra tables. 
    
    Creates and loads data for 3 new Apache Cassandra tables to optimize the 3 user queries as follows:
        1. Get the artist, song title and song's length in the music app history that was heard during a given session_id and item_in_session. (Eg: sessionId = 338, and itemInSession  = 4)
        2. Get the artist, song (sorted by itemInSession) and user (first and last name) for a given user_id and session_id (For Eg: user_id = 10, session_id = 182) 
        3. Get the user name (first and last) from the music app history who listened to a given song (For Eg: 'All Hands Against His Own') 

    Source/Target:
        * Source(s) : Consolidated CSV file named as `event_datafile_new.csv`
        * Target(s) : Apache Cassandra tables modelled for each user requirements 

    The event_datafile_new.csv created in the previous step contains the following columns: 
    - artist 
    - firstName of user
    - gender of user
    - item number in session
    - last name of user
    - length of the song
    - level (paid or free song)
    - location of the user
    - sessionId
    - song title
    - userId
    '''

    #############################################################################################################
    # DATA MODELING STEP
    #############################################################################################################
    # Connect to Cassandra cluster and keyspace
    cluster, session = create_keyspace()
    
    # drop the existing tables if any
    drop_tables(cluster, session)

    '''
    Create tables to optimize the below queries:
    1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
    2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
    3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
    '''
    create_tables(cluster, session)
    print("All tables created successfully!")

    #############################################################################################################
    # DATA LOADING STEP
    #############################################################################################################
    # Load data into table `music_app_history_session_item`
    file = 'event_datafile_new.csv'

    # Open the csv file in read mode.
    with open(file, encoding='utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader)  # skip header

        print("Inserting data into Cassandra tables..")
        # Loop thru each line and insert the data into 3 Cassandra tables .
        for line in csvreader:
            ## INSERT into music_app_history_session_item table
            ## INSERT data into table optimized for query to get artist, song title and song's length in the music app history for a given sessionId and itemInSession
            session.execute(music_app_history_session_item_insert, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))

            ## INSERT into music_app_history_user_session table
            ## INSERT data into table optimized for query to get artist name, song (sorted by itemInSession) and user (first and last name) for a given userid and sessionid
            session.execute(music_app_history_user_session_insert, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))

            ## INSERT into music_app_history_song table
            ## INSERT data into table optimized for query to get every user name (first and last) from the music app history who listened to a given song
            session.execute(music_app_history_song_insert, (line[9], int(line[10]), line[1], line[4]))


    #############################################################################################################
    # DATA VALIDTION STEP
    #############################################################################################################
    '''
    Requirement #1:
        Do a SELECT on `music_app_history_session_item` table based on following conditions to verify the data.
        - Columns to be selected: artist, song title and song's length
        - WHERE Condition : session_id = 338, item_in_session = 4
    '''
    query = "select artist, song, length from music_app_history_session_item where session_id = 338 and item_in_session = 4"
    rows = execute_select_query(cluster, session, query)
    print("Output:")
    for row in rows:
        print(row.artist, row.song, row.length)


    '''
    Requirement #2:
        Do a SELECT on music_app_history_user_session table based on following conditions to verify the data.¶
        - Columns to be selected: name of artist, song(sorted by itemInSession) and user(first and last name)
        - WHERE Condition: user_id = 10, session_id = 182
    '''
    query = "select artist, song, first_name, last_name from music_app_history_user_session where user_id = 10 and session_id = 182"
    rows = execute_select_query(cluster, session, query)
    print("Output:")
    for row in rows:
        print (row.artist, row.song, row.first_name, row.last_name)


    '''
    Requirement #3:
        Do a SELECT on `music_app_history_song` table based on following conditions to verify the data.
        - Columns to be selected: user name (first and last) 
        - WHERE Condition : song 'All Hands Against His Own'
    '''
    query = "select first_name, last_name from music_app_history_song where song='All Hands Against His Own'"
    rows = execute_select_query(cluster, session, query)
    print("Output:")
    for row in rows:
        print (row.first_name, row.last_name)


    # Shutdown session & cluster
    session.shutdown()
    cluster.shutdown()  


if __name__ == "__main__":
    main()
