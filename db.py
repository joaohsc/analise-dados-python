import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PORT = os.getenv("DB_PORT")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

conn = psycopg2.connect(database = DB_NAME, 
                        user = DB_USERNAME, 
                        host= DB_HOST,
                        password = DB_PASSWORD,
                        port = DB_PORT)


def insert_values(sql, value, context):
    try:
        cur = conn.cursor()

        cur.executemany(sql, value)

        rowcount = cur.rowcount
        
        conn.commit()
        cur.close()

        print(f"Insert {context}: {rowcount} linhas inseridos com sucesso!")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Falha no Insert {context}: {error}")

def select_values(table, query="", context=""):
    try:
        cur = conn.cursor()

        cur.execute(f'SELECT * FROM {table} {query};')
        rows = cur.fetchall()
        rowcount = cur.rowcount
        conn.commit()
        cur.close()

        print(f"{context} : {rowcount} registros retornados com sucesso!")

        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Falha no Select {context}: {error}")