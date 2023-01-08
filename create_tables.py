import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    
    """
    - Establishes connection with  the Redshift cluster and gets
    cursor to it. 
    
    - Drops (if exists) the tables. 
    
    - Creates the tables needed. 
    
    - Finally, closes the connection. 
    
    """
    
    # load and save in the object config the codes to connect to 
    # the Redshift cluster
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connect to a Redshift cluster and cursor return connector and cursor
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor() 

    drop_tables(cur, conn) # call function drop_tables define just above
    create_tables(cur, conn) # call function create_tables define just above

    # disconect from the Redshift cluster and cursor
    conn.close()  


if __name__ == "__main__":
    main()