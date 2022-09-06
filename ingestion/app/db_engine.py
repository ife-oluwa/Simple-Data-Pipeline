import psycopg2
import pandas as pd
from sql_queries import create_table_queries, drop_table_queries, fill_table_queries, create_constraints


def create_connections(params):
    '''
    Create new connection with PostgreSQL
    database and return cur and conn object
    :param params: connection string
    '''
    conn = None

    try:
        print('Connecting to PostgreSQL database...')
        conn = psycopg2.connect(**params)
        conn.set_session(autocommit=True)

        cur = conn.cursor()

        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        db_version = cur.fetchone()
        print(db_version)
        return cur, conn

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def close_connection(cur, conn):
    """_summary_
    Close connection
    Args:
        cur: cursor
        conn: connection object
    """
    try:
        cur.close()
        if conn is not None:
            conn.close()
            print('Database connection closed')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def drop_table(cur, conn, table):
    """_summary_
    drop specific tablle
    Args:
        cur (_type_): cursor
        conn (_type_): connection object
        table (_type_): sql table
    """

    query = f"DROP TABLE IF EXISTS {table}"
    print(f'Executing: {query}')
    cur.execute(query)
    conn.commit()


def drop_tables(cur, conn):
    """_summary_
    drop all the tables in the example
    Args:
        cur: cursor
        conn: connection object
    """
    print("Dropping tables")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print("Tables dropped")


def create_tables(cur, conn):
    """_summary_
    create all tables
    Args:
        cur: cursor
        conn: connection object
    """
    print('Creating...')
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("Tables created")


def pg_to_pd(cur, query, columns):
    '''_summary_
    return 'SELECT' result as DataFrame
    Args:
        cur: cursor
        query: SELECT query string
        columns: column names in query
    '''
    try:
        cur.execute(query)
    except (Exception, psycopg2.DatabaseError) as error:
        print(f'Error: {error}')
        return 1

    tuple = cur.fetchall()

    df = pd.DataFrame(tuple, columns=columns)
    return df


def fill_from_staging_all(cur, conn):
    """_summary_
    Fill all records in the tables
    Args:
        cur: cursor
        conn: connection object
    """
    for query in fill_table_queries:
        cur.execute(query)
        conn.commit()
    print("Records were populated from staging")


def check_data(cur, conn, tables):
    """
    Check count of records in tables
    :param cur: cursor
    :param conn: connection object
    :param tables: tables to check
    """
    count_values = {}

    for table in tables:
        count_query = f'SELECT COUNT(*) FROM {table}'

        try:
            cur = conn.cursor()
            count = cur.execute(count_query)
            count_values[table] = count
        except (Exception, psycopg2.DatabaseError) as error:
            print(f'Error: {error}')
            raise
    return count_values


def set_staging(cur, conn, staging_file, columns):
    """_summary_
    Set staging table
    Args:
        cur: cursor
        conn: connection object
        staging_file: csv file
        columns: column names
    """
    print("Copying data from .csv to staging table")
    try:
        all_columns = ', '.join(columns)
        copy_cmd = f'copy staging({all_columns}) from stdout (format csv)'
        with open(staging_file, 'r') as f:
            next(f)
            cur.copy_expert(copy_cmd, f)
        conn.commit()
        print("Staging ready")
    except (psycopg2.Error) as error:
        print(f'Error: {error}')


def set_constraints(cur, conn):
    """_summary_
    Set constraints
    Args:
        cur: cursor
        conn: connection object
    """
    print("Setting constraints")
    for query in create_constraints:
        cur.execute(query)
        conn.commit()
    print("Constraints set")
