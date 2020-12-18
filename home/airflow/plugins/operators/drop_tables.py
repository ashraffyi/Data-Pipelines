from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class DropTablesOperator(BaseOperator):

    ui_color = '#80BD91'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 target_table = '',
                 *args, **kwargs):

        super(DropTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table


    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        drop_table_sql = SqlQueries.table_drop.format(self.target_table)
        #droping tables
        self.log.info("Droping table {}...".format(self.target_table))
        redshift_hook.run(drop_table_sql)