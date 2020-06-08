from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'
    sql_template = 'INSERT INTO public.{table} {query}'

    @apply_defaults
    def __init__(self, conn_id='', query='', table='', *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.query = query
        self.table = table

    def execute(self, context):
        try:
            postgres_hook = PostgresHook(self.conn_id)
            self.log.info('Connected to Redshift')
            insert_sql = self.sql_template.format(table=self.table, query=self.query)
            self.log.info(f'Running query "{insert_sql}" in table {self.table}')
            postgres_hook.run(insert_sql)
            self.log.info(f'Success: {self.task_id} finished for {self.table}')
        except Exception as e:
            self.log.error(e)
