import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# GLOBAL VARIABLES
LOG_DATA = config.get("S3","LOG_DATA")
LOG_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_staging"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop =  "DROP TABLE IF EXISTS artists"
time_table_drop =  "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" 
CREATE TABLE IF NOT EXISTS  events_staging 
(
    artist          varchar,
    auth            varchar, 
    firstName       varchar,
    gender          varchar,   
    itemInSession   int,
    lastName        varchar,
    length          float,
    level           varchar, 
    location        varchar,
    method          varchar,
    page            varchar,
    registration    bigint,
    sessionId       int,
    song            varchar,
    status          int,
    ts              timestamp,
    userAgent       varchar,
    userId          int
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS  songs_staging 
(
    song_id            varchar,
    num_songs          int,
    title              varchar,
    artist_name        varchar,
    artist_latitude    float,
    year               int,
    duration           float,
    artist_id          varchar,
    artist_longitude   float,
    artist_location    varchar
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
    songplay_id int identity(0,1) PRIMARY KEY sortkey, 
    start_time timestamp, 
    user_id int, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int, 
    location varchar, 
    user_agent varchar
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
(
    user_id int PRIMARY KEY distkey, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(
    song_id varchar PRIMARY KEY, 
    title varchar, 
    artist_id varchar distkey, 
    year int, 
    duration float
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
(
    artist_id varchar PRIMARY KEY distkey, 
    name varchar, 
    location varchar, 
    lattitude float, 
    longitude float
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
(
    start_time timestamp PRIMARY KEY sortkey distkey, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int
)
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY events_staging FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {};
""").format(LOG_DATA, IAM_ROLE, LOG_PATH)

staging_songs_copy = ("""
    COPY songs_staging FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                    se.userId as user_id,
                    se.level as level,
                    ss.song_id as song_id,
                    ss.artist_id as artist_id,
                    se.sessionId as session_id,
                    se.location as location,
                    se.userAgent as user_agent
    FROM staging_events se
    JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name)
    AND se.page  ==  'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId as user_id,
                    firstName as first_name,
                    lastName as last_name,
                    gender as gender,
                    level as level
    FROM staging_events
    where userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id as song_id,
                    title as title,
                    artist_id as artist_id,
                    year as year,
                    duration as duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id as artist_id,
                    artist_name as name,
                    artist_location as location,
                    artist_latitude as latitude,
                    artist_longitude as longitude
    FROM staging_songs
    where artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT distinct ts,
                    EXTRACT(hour from ts),
                    EXTRACT(day from ts),
                    EXTRACT(week from ts),
                    EXTRACT(month from ts),
                    EXTRACT(year from ts),
                    EXTRACT(weekday from ts)
    FROM staging_events
    WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
