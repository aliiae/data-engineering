from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """Apply checks to a given tables list."""

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, conn_id='', tables=None, *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.tables = tables or []

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        for table in self.tables:
            try:
                self._has_more_than_zero_rows(table, redshift_hook)
            except Exception as e:
                self.log.error(e)
                raise e
            self.log.info(f'Success: {self.task_id} finished for {table}')

    def _has_more_than_zero_rows(self, table, redshift_hook):
        """Check that the table is not empty."""
        query = f'SELECT COUNT(*) FROM {table}'
        records = redshift_hook.get_records(query)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f'Data quality check failed. {table} returned no results')
        num_records = records[0][0]
        if num_records == 0:
            raise ValueError(f'Data quality check failed. {table} contained 0 rows')
        self.log.info(f'Table {table} has {num_records} rows')
