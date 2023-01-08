import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Creates each table using the queries in `copy_table_queries` list.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Creates each table using the queries in `insert_table_queries` list. 
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """
    - Establishes connection with  the Redshift cluster and gets
    cursor to it. 
    
    - Fill in the staging tables from the data located in AWS S3
    
    - Fill in the set of fact and dimensional tables
    
    - Finally, closes the connection. 
    
    """
    
    # load and save in the object config the codes to connect to 
    # the Redshift cluster and also the path to the files located in AWS S3
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connect to a Redshift cluster and cursor return connector and cursor
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # fill in the staging tables from the data located in AWS S3 
    # and then fill in the set of fact and dimensional tables 
    # from the data in the staging tables
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # close the connection
    conn.close()


if __name__ == "__main__":
    main()