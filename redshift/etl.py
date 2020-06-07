"""Extract, transform and load JSON data using staging tables."""
import configparser

import psycopg2

from sql_queries import copy_table_queries, insert_table_queries

CursorType = 'psycopg2.extensions.cursor'
ConnectionType = 'psycopg2.extensions.connection'


def load_staging_tables(cur: CursorType, conn: ConnectionType):
    """Load staging tables.

    Args:
        cur (CursorType): Cursor connected to the database.
        conn (ConnectionType): Database connection.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur: CursorType, conn: ConnectionType):
    """Execute insertion queries.

    Args:
        cur (CursorType): Cursor connected to the database.
        conn (ConnectionType): Database connection.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Populate the Redshift database."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        'host={0} dbname={1} user={2} password={3} port={4}'.format(
            *config['CLUSTER'].values(),
        ),
    )
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == '__main__':
    main()
