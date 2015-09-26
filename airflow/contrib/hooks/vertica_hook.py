from vertica_python import connect
from pyvertica.batch import VerticaBatch

from airflow.hooks.dbapi_hook import DbApiHook

class VerticaHook(DbApiHook):
    '''
    Interact with Vertica.
    '''

    conn_name_attr = 'vertica_conn_id'
    default_conn_name = 'vertica_default'
    supports_autocommit = True

    def get_conn(self):
        """
        Returns verticaql connection object
        """
        conn = self.get_connection(self.vertica_conn_id)
        conn_config = {
            "user": conn.login,
            "password": conn.password,
            "database": conn.schema,
        }

        conn_config["host"] = conn.host or 'localhost'
        if not conn.port:
            conn_config["port"] = 5433
        else:
            conn_config["port"] = int(conn.port)

        conn = connect(**conn_config)
        return conn

    def split_rows(rows, split_size):
        """Yield rows, split_size at a time."""
        for i in xrange(0, len(rows), split_size):
            yield rows[i:i+split_size]

    def insert_rows(self, table, rows, target_fields=[], commit_every=1000):
        """
        A generic way to insert a set of tuples into a table,
        the whole set of inserts is treated as one transaction
        """

        conn = self.get_conn()

        batch = VerticaBatch(
            table_name=table,
            truncate=False,
            column_list=target_fields,
            copy_options={'DELIMITER': ',','ENCLOSED BY': "'"},
            connection=conn
        )
        
        rows_generator = self.split_rows(rows,commit_every)

        i = 0
        for rows in rows_generator:
            batch.insert_lists(rows)
            batch.commit()
            batch_size = len(rows)
            i += batch_size
            if batch_size == commit_every:
                logging.info(
                    "Loaded {i} into {table} rows so far".format(**locals()))

        logging.info(
            "Done loading. Loaded a total of {i} rows".format(**locals()))

        conn.close()
        
