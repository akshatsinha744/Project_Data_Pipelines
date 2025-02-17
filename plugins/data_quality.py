from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test_cases=None,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_cases = test_cases if test_cases is not None else []

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Running data quality checks")
        
        for test in self.test_cases:
            sql = test['sql']
            expected = test['expected']
            self.log.info(f"Executing: {sql}")
            records = redshift.get_records(sql)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. No results for query: {sql}")
            result = records[0][0]
            if result != expected:
                raise ValueError(f"Data quality check failed. Expected {expected} but got {result}")
            self.log.info(f"Data quality check passed: {sql}")
        
        self.log.info("All data quality checks passed!")
