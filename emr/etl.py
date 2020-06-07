import configparser
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_timestamp

from sql_queries import (
    select_artists,
    select_users,
    select_time,
    select_songplays,
    select_songs,
    song_temp_view,
    log_temp_view,
)

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

# INPUT_DATA = 's3a://udacity-dend/'
INPUT_DATA = 'data/'
OUTPUT_DATA = 'analytics'
SONG_DATA_PATTERN = INPUT_DATA + 'song_data/*/*/*/*.json'
LOG_DATA_PATTERN = INPUT_DATA + 'log-data/*/*/*.json'


def create_spark_session() -> SparkSession:
    spark = SparkSession.builder.config(
        'spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0'
    ).getOrCreate()
    return spark


def process_song_data(spark: SparkSession, input_data: str, output_data: str):
    """Process and create songs, artists tables saved as parquet."""
    _read_songs_data(spark, SONG_DATA_PATTERN)

    _write_songs(output_data, spark)
    _write_artists(output_data, spark)


def process_log_data(spark: SparkSession, input_data: str, output_data: str):
    """Process and create users, time, songplay tables saved as parquet."""
    _read_log_data(spark, LOG_DATA_PATTERN)

    _write_users(output_data, spark)
    _write_time(output_data, spark)

    _read_songs_data(spark, SONG_DATA_PATTERN)
    _write_songplays(output_data, spark)


def main():
    """Read data from S3, process the data using Spark, and write it back to S3."""
    spark = create_spark_session()

    process_song_data(spark, INPUT_DATA, OUTPUT_DATA)
    process_log_data(spark, INPUT_DATA, OUTPUT_DATA)

    spark.stop()


def _write_songs(output_data: str, spark: SparkSession):
    """Save songs data partitioned by year and artist_id in the parquet format."""
    songs_table = spark.sql(select_songs)
    songs_table.write.partitionBy(['year', 'artist_id']).parquet(
        os.path.join(output_data, 'songs.parquet')
    )


def _write_artists(output_data: str, spark: SparkSession):
    """Save artists data partitioned by year and artist_id in the parquet format."""
    artists_table = spark.sql(select_artists)
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'))


def _write_users(output_data: str, spark: SparkSession):
    """Save users data in the parquet format.

    Keeps only the latest records to reflect the current user level (free/paid).
    """
    users_table = spark.sql(select_users)
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'))


def _write_time(output_data: str, spark: SparkSession):
    """Save time data partitioned by year and month in the parquet format."""
    time_table = spark.sql(select_time)
    time_table.write.partitionBy(['year', 'month']).parquet(
        os.path.join(output_data, 'time.parquet')
    )


def _write_songplays(output_data: str, spark: SparkSession):
    songplays_table = spark.sql(select_songplays)
    songplays_table.write.partitionBy(['year', 'month']).parquet(
        os.path.join(output_data, 'songplays.parquet')
    )


def _read_songs_data(spark: SparkSession, path: str) -> DataFrame:
    song_df = spark.read.json(path)
    song_df.createOrReplaceTempView(song_temp_view)
    return song_df


def _read_log_data(spark: SparkSession, path: str) -> DataFrame:
    df = spark.read.json(path)
    df = df.filter("page == 'NextSong'")  # filter by actions for song plays
    df = df.withColumn('start_time', to_timestamp(df.ts / 1000))
    df.createOrReplaceTempView(log_temp_view)
    return df


if __name__ == '__main__':
    main()
