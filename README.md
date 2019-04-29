## <font color=blue>Project Name</font>
Creation, Data Modeling and Data loading of Apache Cassandra database for Song Play Analysis.

## <font color=blue>Project Description & Purpose</font>
A startup called **Sparkify** wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

The purpose of this project is to create a Apache Cassandra database with optimized tables which can create queries on song play data to answer the questions, and wish to bring you on the project. The database should be modeled in such a way to optimize the queries which the users will be running. Subsequently, the ETL pipeline needs to be built to load the Song play event data into the created database which will be used by the analytics team for further analysis. 


## <font color=blue>Source(s)</font>

In this project, the data is extracted from a dataset of CSV files. 
* event data

#### Event Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in CSV format and contains details of each songplay event such as artist name, song played, duration of song, user id, user name, session id, item in session etc.
The directory of CSV files partitioned by date. For example, here are filepaths to two files in this dataset.

`event_data/2018-11-08-events.csv`
`event_data/2018-11-09-events.csv`


And below is an example of what a single event file looks like.
![event dataset](images/image_event_datafile_new.jpg "event dataset")
  

## <font color=blue>Target(s)</font>
The data extracted from Source CSV files are loaded into a new Apache Cassandra database, whose keyspace named as `sparkify` which is optimized for queries on song play analysis. This includes the following tables.

Mainly, there are 3 different queries which Sparkify users will be running. They are as follows:

* 1. Get the artist, song title and song's length in the music app history that was heard during  a given session and item in session (Eg: sessionId = 338, and itemInSession  = 4)


* 2. Get only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for a given user and session (Eg: userid = 10, sessionid = 182)
    

* 3. Get every user name (first and last) in my music app history who listened to a given song (Eg: 'All Hands Against His Own')

In order to optimize the above queries, the event data is loaded into 3 different tables (1 for each of the above query). Each of those 3 tables are partitioned and clustered on appropriate keys which will aid in faster retrieval and optimization while running the respective queries.

The Target tables created to address the above 3 queries are as follows:

#### Tables
1. **music_app_history_session_item** - Gives the events for the given session and item in session
    * artist_song, length
2. **music_app_history_user_session** - Gives the events for given user and session
    * artist, song, first_name, last_name
3. **music_app_history_song** - Gives the events for a given song
    * first_name, last_name

## <font color=blue>ETL Process</font>

In computing, extract, transform, load (ETL) is the general procedure of copying data from one or more sources into a destination system which represents the data differently from the source(s). 

1. Data extraction (**Extract**) involves extracting data from homogeneous or heterogeneous sources; 
2. Data transformation (**Transform**) processes data by data cleansing and transforming them into a proper storage format/structure for the purposes of querying and analysis; 
3. Finally, Data loading (**Load**) describes the insertion of data into the final target database such as an operational data store, a data mart, or a data warehouse.

#### ETL - Extract Step
Data is extracted from 2 set of files which are of JSON file format. 
* event data

#### ETL - Transform Step
There is no special transformation logic done for this process. The data retrieved from Source files are passed as it is to target tables.

#### ETL - Load Step
In the Load step, the extracted and transformed data is loaded into the Apache Cassandra tables in sparkify keyspace. 

## <font color=blue>Project Design</font>
### Part I. ETL Pipeline:
Pre-Process the Source CSV files and create a consolidated CSV file for loading Apache Cassandra tables.

#### Source/Target:
    - Sources : Event files dataset (of CSV format)
    - Target(s) : Consolidated CSV file named as event_datafile_new.csv
    
### Part II. Creation, Modeling and Loading of Apache Cassandra tables. 
#### Create 3 new Apache Cassandra tables to optimize the following 3 user queries:
    1. Get the artist, song title and song's length in the music app history that was heard during a given session_id and item_in_session. (Eg: sessionId = 338, and itemInSession  = 4)
    
    2. Get the artist, song (sorted by itemInSession) and user (first and last name) for a given user_id and session_id (For Eg: user_id = 10, session_id = 182) 

    3. Get the user name (first and last) from the music app history who listened to a given song (For Eg: 'All Hands Against His Own') 

#### Source/Target:
    - Source(s) : Consolidated CSV file named as event_datafile_new.csv
    - Target(s)  Apache Cassandra tables modelled for each user requirements 

The <font color=red>event_datafile_new.csv</font> created in the previous step contains the following columns: 
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

## <font color=blue>Project execution steps</font>
This project can be run either via Jupyter Notebook or by standalone Python scripts.

Run the following scripts in the mentioned order.
1. Run using Jupyter Notebook

    i) Run Python notebook `Project_1B_etl_and Cassandra_load.ipynb`

2. Run using Python scripts

    i) Run `etl_pipeline.py`
    
    ii) Run `apache_cassandra_load.py` (This file invokes `sql_queries.py` to create database and tables)


## <font color=blue>Data Validation</font>

### 1. Get the artist, song title and song's length in the music app history that was heard for session_id = 338 and item_in_session  = 4
#### *Query*:
`select * from music_app_history_session_item where session_id = 338 and item_in_session = 4`

#### *Output*:
artist | song | length
------|--------- |-----------
Faithless|Music Matters (Mark Knight Dub)|495.30731201171875

### 2. Get the artist, song (sorted by itemInSession) and user (first and last name) for user_id = 10 and  session_id = 182

#### *Query*:
`select artist, song, first_name, last_name from music_app_history_user_session where user_id = 10 and session_id = 182`

#### *Output*:
artist | song | length
------|--------- |-----------
Down To The Bone | Keep On Keepin' On | Sylvie | Cruz
Three Drives | Greece 2000 | Sylvie | Cruz
Sebastien Tellier | Kilometer | Sylvie | Cruz
Lonnie Gordon | Catch You Baby (Steve Pitron & Max Sanna Radio Edit) | Sylvie | Cruz


### 3. Get the user name (first and last) from the music app history who listened to the song 'All Hands Against His Own'

#### *Query*:
`select first_name, last_name from music_app_history_song where song='All Hands Against His Own'`

#### *Output*:
first_name | last_name
------|---------
Jacqueline | Lynch
Tegan | Levine
Sara | Johnson

## <font color=blue>Environment and Skills</font> 
- Python, Ipython
- Cassandra, cql

## <font color=blue>References</font> 
https://www.xenonstack.com/blog/overview-types-nosql-databases/
https://docs.datastax.com/en/cql/3.3/cql/cql_using/useAboutCQL.html
https://docs.datastax.com/en/cql/3.3/cql/cql_using/useCompoundPrimaryKey.html
https://stackoverflow.com/questions/24949676/difference-between-partition-key-composite-key-and-clustering-key-in-cassandra
https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cql_data_types_c.html
https://www.tutorialspoint.com/cassandra/cassandra_cql_datatypes.htm
https://www.markdownguide.org/
https://guides.github.com/features/mastering-markdown/
https://github.com/adam-p/markdown-here/wiki/Markdown-Cheatsheet
https://en.wikipedia.org/wiki/Extract,_transform,_load