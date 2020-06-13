import configparser
from typing import List

import psycopg2

config = configparser.ConfigParser()
config.read("credentials.cfg")

IAM_ROLE_ARN = config.get("AWS", "ARN")

CursorType = "psycopg2.extensions.cursor"
ConnectionType = "psycopg2.extensions.connection"

create_table_queries = [
    """
    create table if not exists `time`
    (
        date    date not null primary key,
        day     INTEGER,
        week    INTEGER,
        month   INTEGER,
        year    INTEGER,
        weekday INTEGER
    );
    """,
    """
    create table if not exists property
    (
        id       BIGINT identity (0, 1) not null primary key,
        type     VARCHAR not null,
        is_new   BOOLEAN,
        duration VARCHAR
    );
    """,
    """
    create table if not exists postcode
    (
        code      VARCHAR(7) not null primary key,
        district  VARCHAR    not null,
        country   VARCHAR    not null,
        latitude  FLOAT      not null,
        longitude FLOAT      not null
    );""",
    """
    create table if not exists sale
    (
    
        id          VARCHAR(36) not null primary key,
        price       INTEGER     not null,
        date        date        not null references time (date),
        property_id INTEGER     not null references property (id),
        postcode    VARCHAR(7)  not null references postcode (code),
        address     varchar,
        year        INTEGER,
        month       INTEGER
    );
    """,
]


def copy_tables(cur: CursorType, conn: ConnectionType, table_and_buckets=List[str]):
    """Copy parquet files into Redshift."""
    copy_query = """
    COPY {table}
    FROM '{bucket}'
    IAM_ROLE {iam_role}
    FORMAT AS PARQUET;
    """
    for table, bucket in table_and_buckets:
        query = copy_query.format(table=table, bucket=bucket, iam_role=IAM_ROLE_ARN)
        cur.execute(query)
        conn.commit()


def create_tables(cur: CursorType, conn: ConnectionType):
    """Create tables in Redshift."""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Populate the Redshift database."""
    conn = psycopg2.connect(
        "host={0} dbname={1} user={2} password={3} port={4}".format(
            *config["CLUSTER"].values(),
        ),
    )
    cur = conn.cursor()
    create_tables(cur, conn)
    copy_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()
