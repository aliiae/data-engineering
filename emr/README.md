# Sparkify Example - Data Lake (EMR)

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.
The analytics team is particularly interested in understanding what songs users are listening to.
Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Running

To run the scripts locally, you will need access to an AWS Redshift cluster.
Fill out the credentials in `dl.cfg` following the template. Make sure your cluster is in the us-west-2 region if you plan to use the given JSON data.

To create and populate the tables, run in the terminal:

```bash
python3 etl.py
```

## Project Structure

### Data

- `data/song_data` contains JSON files with song metadata (one JSON per song).
- `data/log_data` contains JSON logs of Sparkify's user activity, partitioned by year and month (one JSON per day).

### Code

- **etl.py** reads data from S3, processes that data using Spark, and writes them back to S3.
- **dl.cfg** contains AWS credentials.

## Data model

![UML Diagram](uml.png)

### Star Schema

The data model uses the star schema optimized for queries on song play analysis.

#### Fact Table

- **songplays** - records in log data associated with song plays
  - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables

- **user** - users in the app
  - user_id, first_name, last_name, gender, level
- **song** - songs in music database
  - song_id, title, artist_id, year, duration
- **artist** - artists in music database
  - artist_id, name, location, latitude, longitude
- **time** - timestamps of records in songplays broken down into specific units
  - start_time, hour, day, week, month, year, weekday

## Pipeline

The pipelines steps are as follows:

1. Read song_data and load_data from S3.
2. Transform them to create five different tables.
3. Writes them to partitioned parquet files in table directories on S3.
