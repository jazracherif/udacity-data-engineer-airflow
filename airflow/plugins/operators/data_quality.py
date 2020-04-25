from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    has_rows = """
        SELECT count(*) 
          FROM {}
    """
    @apply_defaults
    def __init__(self,
                 conn_id,
                 tables: [str],
                 *args, **kwargs):
        """ Operator for checking the data quality. This currently checks that
        each table in the list has some rows in it, raises a ValueError if not

        :param conn_id: the Postgres Connection Id to use
        :param tables (list): List of tables to check
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('DataQualityOperator')

        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        for table in self.tables:
            count = redshift.get_first(DataQualityOperator.has_rows.format(table))

            logging.info(f"{table} has {count[0]} rows")
            if count[0] == 0:
                raise ValueError("Table Empty")
