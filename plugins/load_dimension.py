from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 mode="append",
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.mode = mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.mode == 'truncate-insert':
            self.log.info(f"Clearing data from {self.table} before inserting new data")
            redshift.run(f"DELETE FROM {self.table}")
        
        self.log.info(f"Inserting data into {self.table} dimension table")
        insert_sql = f"INSERT INTO {self.table} {self.sql_query}"
        redshift.run(insert_sql)
        self.log.info(f"Successfully loaded data into {self.table}")
