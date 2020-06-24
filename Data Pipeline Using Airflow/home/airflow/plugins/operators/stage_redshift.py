import logging

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql="""
        copy {}.{}
        from '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        BLANKSASNULL  
        EMPTYASNULL 
        TRUNCATECOLUMNS 
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 schema="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 jsonpath="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.schema=schema
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.jsonpath = jsonpath

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("clearing data from destination redshift table")
        redshift.run("DELETE FROM {}.{}".format(self.schema,self.table))
        
        self.log.info("copying data from s3 to redshift")
        #rendered_key = self.s3_key.format(**context)
        s3_path= "s3://{}/{}".format(self.s3_bucket,self.s3_key)
        if self.jsonpath != "auto":
            jsonpath = "s3://{}/{}".format(self.s3_bucket, self.jsonpath)
        else:
            jsonpath = self.jsonpath
        formatted_sql= StageToRedshiftOperator.copy_sql.format(
            self.schema,
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            jsonpath
        )
        redshift.run(formatted_sql)
            
        





