from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="auto",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Clearing data from Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")
        self.log.info(f"Copying data from S3 to Redshift table {self.table}")
        copy_sql = f'''
            COPY {self.table}
            FROM 's3://{self.s3_bucket}/{self.s3_key}'
            ACCESS_KEY_ID '<your-access-key>'
            SECRET_ACCESS_KEY '<your-secret-key>'
            FORMAT AS JSON '{self.json_path}';
        '''
        redshift.run(copy_sql)
        self.log.info(f"Successfully staged data from S3 to {self.table}")
