import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 insert_sql="",
                 schema="",
                 table="",
                 append_only=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.insert_sql=insert_sql
        self.schema=schema
        self.table=table
        self.append_data = append_only

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info("Loading data into {} fact table".format(self.table))
        
        if self.append_data :
            sql_statement = 'INSERT INTO %s.%s (%s)' % (self.schema,self.table_name,self.insert_sql)
            redshift_hook.run(sql_statement)
        else:
            self.log.info("Deleting data from {}.{} fact table".format(self.schema,self.table))
            redshift_hook.run("Delete from {}.{}".format(self.schema,self.table))
            insert_sql_stmt= "Insert INTO {}.{}({})".format(self.schema,self.table,self.insert_sql)
            redshift_hook.run(insert_sql_stmt)
