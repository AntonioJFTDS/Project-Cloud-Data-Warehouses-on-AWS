import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"

song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"
user_table_drop = "DROP table IF EXISTS users"
songplay_table_drop = "DROP table IF EXISTS songplays"

# CREATE TABLES


staging_events_table_create= ("""
    
    CREATE TABLE IF NOT EXISTS staging_events (
    
    artist VARCHAR(MAX),
    auth VARCHAR(MAX),
    firstName VARCHAR(MAX),
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR(MAX),
    length decimal,
    level VARCHAR(MAX),
    location VARCHAR(MAX),
    method VARCHAR(MAX),
    page VARCHAR(MAX),
    registration VARCHAR(MAX),
    sessionId INT,
    song VARCHAR(MAX),
    status INT,
    ts BIGINT,
    userAgent VARCHAR(MAX),
     userId BIGINT 
    );
""")

staging_songs_table_create = ("""
    
    CREATE TABLE IF NOT EXISTS staging_songs (
    
    num_songs int,
    artist_id varchar,
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year int
    );
""")

songplay_table_create = ("""
    
    CREATE TABLE IF NOT EXISTS songplays (
    
    songplay_id int IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp NOT NULL sortkey distkey,
    user_id int NOT NULL,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id BIGINT,
    location varchar,
    user_agent varchar,
    FOREIGN KEY (start_time) REFERENCES time(start_time),
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (song_id) REFERENCES songs(song_id),
    FOREIGN KEY (artist_id) REFERENCES artists(artist_id)    
    );
""")

user_table_create = ("""
    
    CREATE TABLE IF NOT EXISTS users (
    
    user_id varchar PRIMARY KEY sortkey, 
    first_name varchar NOT NULL,
    last_name varchar,
    gender char(1),
    level varchar
    )
    diststyle all;
""")

song_table_create = ("""
    
    CREATE TABLE IF NOT EXISTS songs (
    
    song_id varchar PRIMARY KEY sortkey,
    title varchar NOT NULL,
    artist_id varchar,
    year int,
    duration float
    )
    diststyle all;
""")

artist_table_create = ("""
    
    CREATE TABLE IF NOT EXISTS artists (
    
    artist_id varchar PRIMARY KEY sortkey ,
    name varchar NOT NULL,
    location varchar,
    latitude float, 
    longitude float
    )
    diststyle all;
""")

time_table_create = ("""
    
    CREATE TABLE IF NOT EXISTS time (
    
    start_time timestamp PRIMARY KEY sortkey, 
    hour int, 
    day int, 
    week int,
    month int, 
    year int, 
    weekday varchar
    )
    diststyle all;
""")

# STAGING TABLES
staging_songs_copy = ("""
    copy {} from '{}' 
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2';
""".format("staging_songs",config.get("S3","SONG_DATA"),config.get("IAM_ROLE","ARN")))

staging_events_copy = ("""
    copy {} from '{}' 
    credentials 'aws_iam_role={}'
    json '{}'
    region 'us-west-2';
""".format("staging_events",config.get("S3","LOG_DATA"),config.get("IAM_ROLE","ARN"),config.get("S3","LOG_JSONPATH")))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
    SELECT  DISTINCT TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' as start_time,
            userId AS user_id,
            level AS level,
            song_id AS song_id,
            artist_id AS artist_id,
            sessionId AS session_id,
            location AS location,
            userAgent AS user_agent
    FROM staging_events
    JOIN staging_songs 
    ON (artist = artist_name AND song = title)
    WHERE page = "NextSong"
""")

user_table_insert = ("""

    INSERT INTO users (user_id,first_name,last_name,gender,level)
    SELECT  DISTINCT userId AS user_id,
            firstName AS first_name,
            lastName AS last_name,
            gender AS gender,
            level AS level
    FROM staging_events
    WHERE userId IS NOT NULL;

""")

song_table_insert = ("""

    INSERT INTO songs (song_id,title,artist_id,year,duration)
    SELECT  DISTINCT song_id AS song_id,
            title AS title,
            artist_id AS artist_id,
            year AS year,
            duration AS duration
    FROM staging_songs;

""")

artist_table_insert = ("""

    INSERT INTO artists (artist_id,name,location,latitude,longitude)
    SELECT  DISTINCT artist_id AS artist_id,
            artist_name AS name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS  longitude   
    FROM staging_songs;

""")

time_table_insert = ("""

    INSERT INTO time (start_time,hour,day,week,month,year,weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' as start_time,
            EXTRACT(hour FROM start_time) AS hour,
            EXTRACT(day FROM start_time) AS day,
            EXTRACT(week FROM start_time) AS week,
            EXTRACT(month FROM start_time) AS month,
            EXTRACT(year FROM start_time) AS year,
            EXTRACT(weekday FROM start_time) AS weekday            
    FROM staging_events
    WHERE ts IS NOT NULL;

""")

# QUERY LISTS
drop_table_queries=[songplay_table_drop,
                    song_table_drop,
                    artist_table_drop,
                    time_table_drop,
                    user_table_drop
                   ]

create_table_queries = [user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create,
                        songplay_table_create]

copy_table_queries = [staging_songs_copy,
                      staging_events_copy]

insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert
                       ]
