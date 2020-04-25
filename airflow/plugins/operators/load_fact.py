from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

 
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    insert_sql = """
        INSERT INTO {}
            {}
        """
    @apply_defaults
    def __init__(self,
                 conn_id,
                 dest_tbl,
                 query,
                 *args, **kwargs):
        """ Operator for Loading Fact Tables

        :param conn_id: the Postgres Connection Id to use
        :param dest_tbl: The name of the destination table
        :param query: the SQL query that generates the dataset
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.query = query
        self.dest_tbl = dest_tbl


    def execute(self, context):
        self.log.info('Running LoadFactOperator for {}'.format(self.dest_tbl))

        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        redshift.run(LoadFactOperator.insert_sql.format(self.dest_tbl, self.query))