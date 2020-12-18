from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 sql = '',
                 table = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table


    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        #droping tables
        self.log.info("Droping table {}...".format(self.table))
        drop_table = 'DELETE FROM {}'.format(self.table)
        redshift_hook.run(drop_table)
        
        #create_tables
        self.log.info("Createing table {}...".format(self.table))
        create_table = 'INSERT INTO {} ({})'.format(self.table, self.sql)
        redshift_hook.run(create_table)
        
        self.log.info("Done loding table: {}".format(self.table))
