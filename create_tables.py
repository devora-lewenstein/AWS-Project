''' A script to create tables from the aws s3
    -----------------------------------------
    '''


"""Spreadsheet Column Printer

This script allows the user to create all the needed tables for our star schema.

This script requires that `configpaser` and 'psycopg2' be installed within the Python
environment you are running this script in.

This file can also be imported as a module and contains the following
functions:

    * drop_tables - drops tables just in case it was creates already
    * create_tables - create necessary tables for star schema 
    * main - the main function of the script
"""

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
#from AWS_Redshift_Cluster_Setup import conn_string


def drop_tables(cur, conn):
    ''' Drops tables to ensure never created before
        -------------------------------------------
        
        imported from sql_queries.py
        
        Parameters: cur - sql cursor created in main function below
                    conn - sql connection created in main function below
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    ''' Create tables based need for star schema
        -----------------------------------------
            
        imported from sql_queries.py
        
        Parameters: cur - sql cursor created in main function below
                    conn - sql connection created in main function below
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    ''' Function that creates connection and runs all functions
        --------------------------------------------------------
            
        connects to bucket endpoint based on DWH information saved in dwg.cfg
        than creates a psycopg2 connection
        than creates a cursor to run sql
        run sql code to drop and create tables using connection and cursor created
        closes connectoin
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
#    conn = conn_string
#    conn="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

#annonymous class __name__ to run the main function
if __name__ == "__main__":
    main()
    
                               