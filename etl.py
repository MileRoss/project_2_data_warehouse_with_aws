import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, analytics_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def run_analytics_queries(cur, conn):
    print("\n─── Songplay Analytics ───")
    for question, query in analytics_queries:
        print(f"\n{question}")
        cur.execute(query)
        rows = cur.fetchall()
        for row in rows:
            print(row)
    conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    run_analytics_queries(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()