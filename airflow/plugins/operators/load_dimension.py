from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """Load dimension tables into Redshift. Set append_only to False to delete existing data."""

    ui_color = '#F98866'
    sql_template = 'INSERT INTO public.{table} {query}'

    @apply_defaults
    def __init__(
        self, conn_id='', sql='', table='', append_only=False, *args, **kwargs
    ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.query = sql
        self.table = table
        self.append_only = append_only

    def execute(self, context):
        try:
            postgres_hook = PostgresHook(self.conn_id)
            self.log.info('Connected to Redshift')

            if not self.append_only:
                delete_sql = f'TRUNCATE {self.table}'
                self.log.info(f'Deleting existing data from {self.table}: {delete_sql}')
                postgres_hook.run(delete_sql)

            insert_sql = self.sql_template.format(table=self.table, query=self.query)
            self.log.info(f'Running query "{insert_sql}" in table {self.table}')
            postgres_hook.run(insert_sql)
            self.log.info(f'Success: {self.task_id} finished for {self.table}')
        except Exception as e:
            self.log.error(e)
