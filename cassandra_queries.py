import cassandra
import pandas as pd
import file_processing

# Create a connection to a Cassandra instance 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# Create a session to establish a connection and begin executing queries
session = cluster.connect()

# Create a Keyspace 
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS song_play 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)

# Set KEYSPACE to the keyspace specified above
try:
    session.set_keyspace('song_play')
except Exception as e:
    print(e)

## This section models a table based on the first query that is meant to retrieve the artist's name, the song title and the song's length in the music app history that was heard during \
## the session with sessionId being 338, and itemInSession is 4. 
## The PRIMARY KEY for this table will be a composite key of 'session_id' and 'item_in_session'. They are the strongest combination of all the data that is required from the query \
## and therefore making the combination unique.

### Create the table 'song_played_during_session'
query = "CREATE TABLE IF NOT EXISTS song_played_during_session "
query = query + "( session_id int, item_in_session int, artist_name text, song_title text, song_length decimal, PRIMARY KEY (session_id, item_in_session))"
try:
    session.execute(query)
except Exception as e:
    print(e)       

### retrieve data from the generated csv file

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:

### Assign the INSERT statements into the `query` variable
        query = "INSERT INTO song_played_during_session (session_id, item_in_session, artist_name, song_title, song_length)"
        query = query + "VALUES (%s, %s, %s, %s, %s)"
        ### Assign column elements for each column.
        session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))

### Add in the SELECT statement to verify the data was entered into the table
query = "select artist_name, song_title, song_length from song_played_during_session WHERE session_id=338 and item_in_session=4"
try:
    song_played_during_session = pd.DataFrame(list(session.execute(query)))
except Exception as e:
    print(e)

song_played_during_session

## Model a table based on the second query that will retrieve the name of artist, song title (sorted by itemInSession) and user's first and last name\
## where the userid is equal to 10 and the sessionid is equal to 182.
## The PRIMARY KEY for this table model will consist of a PARTITION KEY made up of both the 'user_id' and 'session_id' and a CLUSTERING COLUMN which is the column 'item_in_session'. \
## The reason for the PARTITION KEY columns is that will both be required in the select statement's WHERE clause.
## The CLUSTERING COLUMN item_in_session is what the song_title will be sorted by.

### Create the table 'song_listened_by_user'
query = "CREATE TABLE IF NOT EXISTS song_listened_by_user "
query = query + "( user_id int, session_id int, item_in_session int, user_first_name text, user_last_name text, song_title text, artist_name text, PRIMARY KEY ((user_id, session_id), item_in_session))"
try:
    session.execute(query)
except Exception as e:
    print(e)

### retrieve data from the generated csv file
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:

### Assign the INSERT statements into the `query` variable
        query = "INSERT INTO song_listened_by_user (user_id, session_id, item_in_session, user_first_name, user_last_name, song_title, artist_name)"
        query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        ### Assign column elements for each column.
        session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[1], line[4], line[9], line[0]))

# add the SELECT statement to verify the data in the table
query = "select artist_name, song_title, user_first_name, user_last_name from song_listened_by_user WHERE user_id=10 and session_id=182"
try:
    # set the query result in a DataFrame
    song_played_by_user = pd.DataFrame(list(session.execute(query)))
except Exception as e:
    print(e)

# print the items from the dataframe 
song_played_by_user

## Model a table based on a query that will retrieve every user's name (first and last) in the music app history who listened to the song 'All Hands Against His Own'
## The PRIMARY KEY for this model will be a composite primary key consisting of the 'song_title' and 'user_id'. They both provide the strongest combination of the data that \
## is getting stored in the table.

### Create the table 'users_who_played_song'
query = "CREATE TABLE IF NOT EXISTS users_who_played_song "
query = query + "( song_title text, user_id int, user_first_name text, user_last_name text, PRIMARY KEY (song_title, user_id))"
try:
    session.execute(query)
except Exception as e:
    print(e)

### retrieve data from the generated csv file
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:

### Assign the INSERT statements into the `query` variable
        query = "INSERT INTO users_who_played_song (song_title, user_id, user_first_name, user_last_name)"
        query = query + "VALUES (%s, %s, %s, %s)"
        ### Assign column elements for each column.
        session.execute(query, (line[9], int(line[10]), line[1], line[4]))

### add the SELECT statement to verify the data in the table
query = "select user_first_name, user_last_name from users_who_played_song WHERE song_title='All Hands Against His Own'"
try:
    # set the query result in a DataFrame
    users_who_played_song = pd.DataFrame(list(session.execute(query)))
except Exception as e:
    print(e)

### print the items from the dataframe 
users_who_played_song

## Drop the tables before closing out the sessions
### Drop table 'song_played_in_session'
query = "DROP TABLE song_played_during_session"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

#### Drop table 'song_played_by_user'
query = "DROP TABLE song_listened_by_user"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

#### Drop table 'users_who_played_song'
query = "DROP TABLE users_who_played_song"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

### Close out the session
session.shutdown()

### close out the Cluster
cluster.shutdown()