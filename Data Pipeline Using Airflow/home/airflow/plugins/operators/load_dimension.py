import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 insert_sql="",
                 schema="",
                 table="",
                 append_only=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.insert_sql=insert_sql
        self.schema=schema
        self.table=table
        self.append_data = append_only

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Loading data into {}.{} dimension table".format(self.schema,self.table))
        
        if self.append_data == True:
            sql_statement = 'INSERT INTO %s.%s %s' % (self.schema,self.table_name, self.insert_sql)
            redshift.run(sql_statement)
        else:
            self.log.info("Deleting data from {}.{} dimension table".format(self.schema,self.table))
            redshift.run("Delete from {}.{}".format(self.schema,self.table))
            insert_sql_stmt= 'INSERT INTO %s.%s %s' % (self.schema,self.table, self.insert_sql)
            redshift.run(insert_sql_stmt)
