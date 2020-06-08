from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from helpers import SqlQueries

REDSHIFT_CONN_ID = 'redshift'
AWS_CREDENTIALS_ID = 'aws_credentials'
INPUT_BUCKET = 'udacity-dend'

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 5, 8),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=300),
    'catchup': False,
}
fact_table_name_and_query = ('songplays', SqlQueries.songplay_table_insert)
dim_tables_name_to_query = {
    'users': SqlQueries.user_table_insert,
    'songs': SqlQueries.song_table_insert,
    'artists': SqlQueries.artist_table_insert,
    'time': SqlQueries.time_table_insert,
}

dag = DAG(
    'udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_tables = PostgresOperator(
    task_id='Create_tables',
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql='/create_tables.sql',
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    s3_bucket=INPUT_BUCKET,
    s3_key='log-data/{execution_date.year}/{execution_date.month:02d}',
    table='staging_events',
    file_format="JSON 's3://udacity-dend/log_json_path.json'",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    s3_bucket=INPUT_BUCKET,
    s3_key='song-data/*/*',
    table='staging_songs',
    file_format="JSON 'auto'",
)

load_songplays_table = LoadFactOperator(
    task_id=f'Load_{fact_table_name_and_query[0]}_fact_table',
    dag=dag,
    table=fact_table_name_and_query[0],
    conn_id=REDSHIFT_CONN_ID,
    sql=fact_table_name_and_query[1],
)

dim_operators = [
    LoadDimensionOperator(
        task_id=f'Load_{dim_table_name}_dim_table',
        dag=dag,
        table=dim_table_name,
        conn_id=REDSHIFT_CONN_ID,
        sql=dim_query,
    )
    for dim_table_name, dim_query in dim_tables_name_to_query.items()
]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id=REDSHIFT_CONN_ID,
    tables=list(dim_tables_name_to_query) + [fact_table_name_and_query[0]],
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_tables
create_tables >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> dim_operators
dim_operators + [load_songplays_table] >> run_quality_checks
run_quality_checks >> end_operator
