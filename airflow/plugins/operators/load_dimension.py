from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql = """
        INSERT INTO {}
        {}
    """
    @apply_defaults
    def __init__(self,
                 conn_id,
                 table,
                 query,
                 mode="APPEND",
                 *args, **kwargs):
        """ Operator for Loading Dimension Tables

        :param conn_id: the Postgres Connection Id to use
        :param table: The name of the destination table
        :param query: the SQL query that generates the dataset
        :param mode: the mode, either APPEND or TRUNCATE, the later
            deleting the content of the table before insertion.
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.conn_id = conn_id
        self.query = query
        self.mode = mode
        self.table = table

        if mode not in ["APPEND", "TRUNCATE"]:
            raise "ValueError"

    def execute(self, context):
        self.log.info('LoadDimensionOperator')

        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        if self.mode == "TRUNCATE":
            redshift.run(f"TRUNCATE {self.table}")

        redshift.run(LoadDimensionOperator.insert_sql.format(self.table, self.query))