import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():

    #connect to default database
    conn = psycopg2.connect(host="localhost", dbname="postgres", user="postgres", password="Immortalmk03@")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    #create sparkify database
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING utf8 TEMPLATE template0")

    #close the default connection
    conn.close()

    #connect to sparkify database
    conn = psycopg2.connect(host="localhost", dbname="sparkifydb", user="postgres", password="Immortalmk03@")
    cur = conn.cursor()

    return cur, conn



def drop_table(cur, conn):
    #drop table using the queries in 'drop_table_queries' list
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_table(cur, conn):
    #create table using the queries in 'create_table_queries' list
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()
    drop_table(cur, conn)
    create_table(cur, conn)

    conn.close()


if __name__ == '__main__':
    main()
