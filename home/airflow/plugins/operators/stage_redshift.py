from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from helpers import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 aws_conn_id = 'aws_credentials',
                 table = '',
                 option ='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id =redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.option = option

    def execute(self, context):
        ## AWS setup
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.option == "Song_data":
            bucket = Variable.get('s3_bucket')
            prefix = Variable.get('s3_prefix_song_data')
            s3_path = "s3://{}/{}".format(bucket, prefix)
            self.log.info(f'Preparing to stage data from {s3_path} to {self.table} table')
            copy_format = Variable.get('song_data_json_path')
            copy_sql = SqlQueries.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                copy_format
            )
            self.log.info(f'Coping data from {s3_path} to {self.table} table')
            redshift_hook.run(copy_sql)
            self.log.info(f'Copy successful to stage data from {s3_path} to {self.table} table')
        elif self.option == "Log_data":
            bucket = Variable.get('s3_bucket')
            prefix = Variable.get('s3_prefix_logdata')
            s3_path = "s3://{}/{}".format(bucket, prefix)
            self.log.info(f'Preparing to stage data from {s3_path} to {self.table} table')
            copy_format = Variable.get('Log_data_json_path')
            copy_sql = SqlQueries.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                copy_format
            )
            self.log.info(f'Coping data from {s3_path} to {self.table} table')
            redshift_hook.run(copy_sql)
            self.log.info(f'Copy successful to stage data from {s3_path} to {self.table} table')
        else:
            self.log.info('StageToRedshiftOperator nothing to import')
        self.log.info(f'Copy successful of all s3_bucket')




