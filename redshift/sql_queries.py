"""A collection of SQL queries used across modules."""

import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ROLE_ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
SONG_DATA = config.get('S3', 'SONG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')

# DROP TABLES

staging_events_table_drop = 'DROP TABLE IF EXISTS staging_events;'
staging_songs_table_drop = 'DROP TABLE IF EXISTS staging_songs;'
songplay_table_drop = 'DROP TABLE IF EXISTS songplay;'
user_table_drop = 'DROP TABLE IF EXISTS users;'
song_table_drop = 'DROP TABLE IF EXISTS song;'
artist_table_drop = 'DROP TABLE IF EXISTS artist;'
time_table_drop = 'DROP TABLE IF EXISTS time;'

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events(
    artist VARCHAR,
    auth VARCHAR,
    first_name VARCHAR,
    gender VARCHAR,
    item_session INTEGER,
    last_name VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    session_id INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    user_agent VARCHAR,
    user_id INTEGER
);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INTEGER
);
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplay (
    songplay_id INTEGER IDENTITY (0, 1) NOT NULL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL REFERENCES time (start_time),
    user_id INTEGER NOT NULL REFERENCES users (user_id),
    level VARCHAR,
    song_id VARCHAR REFERENCES song (song_id),
    artist_id VARCHAR REFERENCES artist (artist_id),
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR
);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
   user_id INTEGER NOT NULL PRIMARY KEY,
   first_name VARCHAR,
   last_name VARCHAR,
   gender VARCHAR,
   level VARCHAR
);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS song (
   song_id VARCHAR NOT NULL PRIMARY KEY,
   title VARCHAR,
   artist_id VARCHAR,
   year INTEGER,
   duration FLOAT
);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artist (
   artist_id VARCHAR NOT NULL PRIMARY KEY,
   name VARCHAR,
   location VARCHAR,
   latitude FLOAT,
   longitude FLOAT
);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
   start_time TIMESTAMP NOT NULL PRIMARY KEY,
   hour INTEGER,
   day INTEGER,
   week INTEGER,
   month INTEGER,
   year INTEGER,
   weekday INTEGER
);
"""

# STAGING TABLES

staging_events_copy = f"""
COPY staging_events
FROM {LOG_DATA} iam_role {ROLE_ARN} json {LOG_JSONPATH};
"""
staging_songs_copy = f"""
COPY staging_songs
FROM {SONG_DATA} iam_role {ROLE_ARN} json 'auto';
"""

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplay (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
SELECT TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' as start_time,
    e.user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.session_id,
    e.location,
    e.user_agent
FROM staging_events e,
    staging_songs s
WHERE e.page = 'NextSong'
    AND e.song = s.title
    AND e.artist = s.artist_name
    AND e.length = s.duration;
"""

user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id,
    first_name,
    last_name,
    gender,
    level
FROM staging_events
WHERE page = 'NextSong';
"""

song_table_insert = """
INSERT INTO song (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL;
"""

artist_table_insert = """
INSERT INTO artist (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
"""

time_table_insert = """
INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
SELECT DISTINCT start_time,
    EXTRACT(
        hour
        FROM start_time
    ),
    EXTRACT(
        day
        FROM start_time
    ),
    EXTRACT(
        week
        FROM start_time
    ),
    EXTRACT(
        month
        FROM start_time
    ),
    EXTRACT(
        year
        FROM start_time
    ),
    EXTRACT(
        dayofweek
        FROM start_time
    )
FROM songplay
WHERE start_time IS NOT NULL;
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
