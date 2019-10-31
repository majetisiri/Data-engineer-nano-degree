import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


"""
   Drops the tables if exists.

    INPUTS: 
    * cur the cursor variable
    * conn connection to the redshift database
    """
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


"""
   Creates the tables if does not exists.

    INPUTS: 
    * cur the cursor variable
    * conn connection to the redshift database
    """
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


"""
   This procedure  executes first and connects to the redshift database.
    After connecting to the database drop_tables and create_tables procedures  are called by passing connection and cursor.

    INPUTS: 
    * cur the cursor variable
    * conn connection to the redshift database
    """
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()