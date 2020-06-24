import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table=[""],
                 schema="",
                 Tests={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.schema=schema
        self.redshift_conn_id = redshift_conn_id
        self.tests=Tests

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.table:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.schema}.{table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {self.schema}.{table} returned no results")
            num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.schema}.{table} contained 0 rows")
            logging.info(f"Data quality on table {schema}.{table} check passed with {records[0][0]} records")
        
        
        for check in self.tests:
            actual_result = check.get('test')
            exp_result = check.get('expected_result')
 
            records = redshift_hook.get_records(actual_result)[0]
 
            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(actual_result)
                if error_count > 0:
                    self.log.info('Tests failed')
                    self.log.info(failing_tests)
                    raise ValueError('Data quality check failed')