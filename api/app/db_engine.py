import psycopg2
import pandas as pd


async def create_connection(params):

    conn = None
    try:
        print('Connecting to PostgreSQL database')
        conn = psycopg2.connect(**params)
        conn.set_session(autocommit=True)

        cur = conn.cursor()

        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        db_version = cur.fetchone()
        print(db_version)

        return cur, conn

    except (Exception, psycopg2.DatabaseError) as e:
        return None, None, True, str(e)


def close_connection(cur, conn):
    try:
        cur.close()
        if conn is not None:
            conn.close()
            print('Database connection closed')
    except (Exception, psycopg2.DatabaseError) as e:
        print(e)


def pg_to_pd(cur, query, columns):
    try:
        cur.execute(query)
    except (Exception, psycopg2.DatabaseError) as e:
        print(f'Error: {e}')
        return 1

        tuples = cur.fetchall()
        df = pd.DataFrame(tuples, columns=columns)
        return df
