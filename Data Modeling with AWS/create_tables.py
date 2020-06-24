import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
       Drops staging, fact and dimension tables.
      """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
       Creates staging, fact and dimension tables.
      """
    for query in create_table_queries:
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
    # calls the create_table and drop_table functions.
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()