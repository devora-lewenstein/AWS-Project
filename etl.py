''' A script to copy s3 bucket data into tables and insert info
    -----------------------------------------
    '''


"""Spreadsheet Column Printer

This script is used after create_tables.py runs successfully and is connected to sql_queries.py
It allows the user to extract the  data and insert all the needed information into the tables for create_tables.py
This script requires that `configpaser` and 'psycopg2' be installed within the Python
environment you are running this script in.

This file can also be imported as a module and contains the following
functions:

    * load_staging_tables- extracting the data and copying it into a staging area
    * insert_tables - inserting data from the extracted data into created tables
    * main - the main function of the script
"""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    ''' Loads data for staging
        ----------------------
        
        imported from sql_queries.py
        
        Parameters: cur - sql cursor created in main function below
                    conn - sql connection created in main function below
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    ''' Insert data from loaded tables into star schema tables
        ----------------------
        
        imported from sql_queries.py
        
        Parameters: cur - sql cursor created in main function below
                    conn - sql connection created in main function below
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    ''' Function that creates connection and runs all functions
        --------------------------------------------------------
            
        connects to bucket endpoint based on DWH information saved in dwg.cfg
        than creates a psycopg2 connection
        than creates a cursor to run sql
        run sql code to copy data and insert data into tables using connection and cursor created
        closes connectoin
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

#annonymous class __name__ to run the main function
if __name__ == "__main__":
    main()