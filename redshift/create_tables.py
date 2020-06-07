"""Execute table creation queries."""
import configparser

import psycopg2

from sql_queries import create_table_queries, drop_table_queries

CursorType = 'psycopg2.extensions.cursor'
ConnectionType = 'psycopg2.extensions.connection'


def drop_tables(cur: CursorType, conn: ConnectionType):
    """Drop each table using the queries in `drop_table_queries` list.

    Args:
        cur (CursorType): Cursor connected to the database.
        conn (ConnectionType): Database connection.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur: CursorType, conn: ConnectionType):
    """Create each table using the queries in `create_table_queries` list.

    Args:
        cur (CursorType): Cursor connected to the database.
        conn (ConnectionType): Database connection.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Re-populate the database."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        'host={0} dbname={1} user={2} password={3} port={4}'.format(
            *config['CLUSTER'].values(),
        ),
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == '__main__':
    main()
