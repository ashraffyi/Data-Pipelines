from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):

    ui_color = '#80BD98'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 sql = '',
                 table ='',
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table


    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        #droping tables
        self.log.info("creating table {}...".format(self.table))
        redshift_hook.run(self.sql)