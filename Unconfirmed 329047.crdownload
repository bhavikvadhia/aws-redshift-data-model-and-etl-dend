import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from time import time

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print('following command is being executed: {} ...'.format(query[:30]))
        t0=time()
        cur.execute(query)
        conn.commit()
        t1=time()-t0
        print("=== DONE IN: {0:.2f} sec\n".format(t1))


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print('following command is being executed: {} ...'.format(query[:30]))
        t0=time()
        cur.execute(query)
        conn.commit()
        t1=time()-t0
        print("=== DONE IN: {0:.2f} sec\n".format(t1))


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