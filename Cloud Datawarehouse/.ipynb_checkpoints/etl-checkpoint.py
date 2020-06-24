import configparser
import psycopg2
from sql_queries import copy_table_queries,insert_table_queries


def load_staging_tables(cur, conn):
    """
       Load data from "song/log" files in S3 to staging tables.
      """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        

def insert_tables(cur, conn):
    """
       Using staging tables loads the data into analytics tables on redhsift.
      """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
       Uses the DWH Config file to get credentials of the redshift cluster and connects to cluster 
       using postgresql driver. 
      """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    # calls the load_staging_tables and insert_tables functions.
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
    

    
