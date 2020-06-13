from datetime import datetime, timedelta

import configparser
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from operators import StageToRedshiftOperator, DataQualityOperator

REDSHIFT_CONN_ID = "redshift"
AWS_CREDENTIALS_ID = "aws_credentials"
INPUT_BUCKET = "data"

config = configparser.ConfigParser()
config.read("credentials.cfg")

pwd = "/usr/local/airflow"
spark_submit_command = "/Users/rutargetuser/GoogleDrive/2020-2/Github/postgres-models/capstone/venv/bin/spark-submit"
default_args = {
    "owner": "me",
    "start_date": datetime(1994, 1, 1),
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=300),
    "catchup": False,
}

fact_table_name = "sale"
dim_table_names = ["time", "property", "postcode"]

dag = DAG(
    "uk_house_prices_on_map",
    default_args=default_args,
    description="Load and transform UK house pricing data",
    schedule_interval="@once",
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

process_sale = SparkSubmitOperator(
    task_id="Process_sale_dataset",
    dag=dag,
    application=f"{pwd}/spark_jobs/process_ppd.py",
)

process_postcode = SparkSubmitOperator(
    task_id="Process_postcode_dataset",
    dag=dag,
    application=f"{pwd}/spark_jobs/process_postcode.py",
)

create_sale_tables = PostgresOperator(
    task_id="Create_sale_tables",
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql="/create_sale_tables.sql",
)

create_postcode_tables = PostgresOperator(
    task_id="Create_postcode_tables",
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql="/create_postcode_tables.sql",
)

stage_sale_table = StageToRedshiftOperator(
    task_id=f"Copy_{fact_table_name}_fact_table",
    dag=dag,
    table=fact_table_name,
    conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    s3_bucket=INPUT_BUCKET,
    s3_key=f"{fact_table_name}.csv/*.csv",
    file_format="CSV",
    provide_context=True,
)

dim_operators = [
    StageToRedshiftOperator(
        task_id=f"Copy_{dim_table_name}_dim_table",
        dag=dag,
        table=dim_table_name,
        conn_id=REDSHIFT_CONN_ID,
        aws_credentials_id=AWS_CREDENTIALS_ID,
        s3_bucket=INPUT_BUCKET,
        s3_key=f"{dim_table_name}.csv/*.csv",
        file_format="CSV",
        provide_context=True,
    )
    for dim_table_name in dim_table_names
]

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    conn_id=REDSHIFT_CONN_ID,
    tables=dim_table_names + [fact_table_name],
)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> [process_sale, process_postcode]
process_sale >> create_sale_tables >> dim_operators[:-1] + [stage_sale_table]
process_postcode >> create_postcode_tables >> dim_operators[-1]
dim_operators + [stage_sale_table] >> run_quality_checks
run_quality_checks >> end_operator
