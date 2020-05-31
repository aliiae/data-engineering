import glob
import os
from typing import Callable

import pandas as pd
import psycopg2

from sql_queries import *

CursorType = 'psycopg2.extensions.cursor'
ConnectionType = 'psycopg2.extensions.connection'


def process_song_file(cur: CursorType, filepath: str):
    """
    Inserts a single song's info into the database from the JSON file given in `filepath`.
    Tables affected:
      - songs,
      - artists.
    :param cur: Cursor connected to the database.
    :param filepath: Path to the song's JSON file.
    :return: None.
    """
    df = pd.read_json(filepath, lines=True)
    song_data = list(
        df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    )
    cur.execute(song_table_insert, song_data)
    artist_data = list(
        df[
            [
                'artist_id',
                'artist_name',
                'artist_location',
                'artist_latitude',
                'artist_longitude',
            ]
        ].values[0]
    )
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur: CursorType, filepath: str):
    """
    Inserts a single songplay action data into the database from the JSON file given in `filepath`.
    :param cur: Cursor connected to the database.
    :param filepath: Path to a log JSON file.
    :return: None.
    """
    df = pd.read_json(filepath, lines=True)
    df = df[df.page == 'NextSong']
    insert_times(cur, df)
    insert_users(cur, df)
    insert_songplays(cur, df)


def insert_songplays(cur: CursorType, df: pd.DataFrame):
    """
    Inserts songplay records into the database.
    :param cur: Cursor connected to the database.
    :param df: Dataframe containing songplay columns.
    :return: None.
    """
    for index, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        songplay_data = (
            row['ts'],
            row['userId'],
            row['level'],
            songid,
            artistid,
            row['sessionId'],
            row['location'],
            row['userAgent'],
        )
        cur.execute(songplay_table_insert, songplay_data)


def insert_users(cur: CursorType, df: pd.DataFrame):
    """
    Inserts user records into the database.
    :param cur: Cursor connected to the database.
    :param df: Dataframe containing user columns.
    :return: None.
    """
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)


def insert_times(cur: CursorType, df: pd.DataFrame):
    """
    Inserts time records into the database.
    :param cur: Cursor connected to the database.
    :param df: Dataframe containing timestamps.
    :return: None.
    """
    t = pd.to_datetime(df.ts, unit='ms')
    # insert time data records
    time_data = (
        df.ts,
        t.dt.hour,
        t.dt.day,
        t.dt.week,
        t.dt.month,
        t.dt.year,
        t.dt.weekday,
    )
    column_labels = ('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(
        {label: data for label, data in zip(column_labels, time_data)}
    )
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))


def process_data(
        cur: CursorType,
        conn: ConnectionType,
        filepath: str,
        func: Callable[[CursorType, str], None],
):
    """
    Inserts records from parsed JSON files into the database.
    :param cur: Cursor connected to the database.
    :param conn: Database connection.
    :param filepath: Path to the folder containing JSON files (e.g. logs or song data).
    :param func: Processing function.
    :return: None.
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Connects to the database.
    - Processes song data.
    - Processes log data.
    """
    conn = psycopg2.connect(
        'host=127.0.0.1 dbname=sparkifydb user=student password=student'
    )
    cur = conn.cursor()
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    conn.close()


if __name__ == '__main__':
    main()
