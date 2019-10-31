import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


"""
    This procedure inserts data into the events_staging and songs_staging tables by copying the json data from song data and log data in s3 buckets.

    INPUTS: 
    * cur the cursor variable
    * conn connection to the redshift database
    """
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


"""
    This procedure processes extracts data from the events_staging and songs_staging tables and inserts into songplays, artists, songs, users and time tables.

    INPUTS: 
    * cur the cursor variable
    * conn connection to the redshift database
    """
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()



"""
    This procedure  executes first and connects to the redshift database.
    After connecting to the database load_staging_tables, insert_tables procedures are called by passing connection and cursor.
    """
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()