from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)
    sql_template = """
    COPY {table}
    FROM '{s3_path}'
    ACCESS_KEY_ID '{key_id}'
    SECRET_ACCESS_KEY '{access_key}'
    FORMAT AS {file_format}
    """

    @apply_defaults
    def __init__(
        self,
        conn_id='',
        aws_credentials_id='',
        table='',
        s3_bucket='',
        s3_key='',
        file_format='',
        *args,
        **kwargs,
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.log.info('Connected to AWS')
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info('Connected to Redshift')

        s3_key = self.s3_key.format(**context)
        s3_path = f's3://{self.s3_bucket}/{s3_key}'

        copy_query = self.sql_template.format(
            table=self.table,
            s3_path=s3_path,
            key_id=credentials.access_key,
            access_key=credentials.secret_key,
            file_format=self.file_format,
        )
        self.log.info(f'Running query "{copy_query}" in table {self.table}, s3_path is {s3_path}')
        redshift.run(copy_query)
        self.log.info(f'Success: {self.task_id} finished for {self.table}')
