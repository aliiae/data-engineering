# Sparkify Example - Postgres

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. 
The analytics team is particularly interested in understanding what songs users are listening to. 
Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Data model

The data model uses the star schema optimized for queries on song play analysis.

### Fact Table

- **songplays** - records in log data associated with song plays
  - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

- **users** - users in the app
  - user_id, first_name, last_name, gender, level
- **songs** - songs in music database
  - song_id, title, artist_id, year, duration
- **artists** - artists in music database
  - artist_id, name, location, latitude, longitude
- **time** - timestamps of records in songplays broken down into specific units
  - start_time, hour, day, week, month, year, weekday

## Advantages
The proposed approach has the benefits of a denormalized schema: simpler queries, performance gains in read-only queries, faster aggregations.
