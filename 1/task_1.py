import psycopg2
import json
import csv
from decimal import Decimal


# Function to create the PostgreSQL table
def create_table(conn):
    cursor = conn.cursor()

    # Define the table schema
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS books (
            title TEXT,
            author TEXT,
            genre TEXT,
            pages INTEGER,
            published_year INTEGER,
            isbn TEXT,
            rating REAL,
            views INTEGER
        )
    ''')

    conn.commit()


# Function to populate the PostgreSQL table from CSV
def populate_table_from_csv(conn):
    cursor = conn.cursor()

    with open('task_1_var_08_item.csv', 'r', encoding='utf-8') as csvfile:
        csv_reader = csv.DictReader(csvfile, delimiter=';')

        for row in csv_reader:
            cursor.execute('''
                INSERT INTO books
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                row['title'],
                row['author'],
                row['genre'],
                int(row['pages']),
                int(row['published_year']),
                row['isbn'],
                float(row['rating']),
                int(row['views'])
            ))

    conn.commit()


def execute_queries_and_save_to_json(conn):
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM books ORDER BY pages LIMIT %s', (8 + 10,))
    query1_result = cursor.fetchall()
    with open('query1_result.json', 'w', encoding='utf-8') as json_file:
        json.dump(query1_result, json_file, indent=2, default=str, ensure_ascii=False)

    cursor.execute('SELECT SUM(pages), MIN(pages), MAX(pages), AVG(pages) FROM books')
    query2_result = cursor.fetchone()
    with open('query2_result.json', 'w', encoding='utf-8') as json_file:
        json.dump({'sum': float(query2_result[0]), 'min': float(query2_result[1]),
                   'max': float(query2_result[2]), 'average': float(query2_result[3])}, json_file, indent=2, ensure_ascii=False)

    cursor.execute('SELECT genre, COUNT(*) FROM books GROUP BY genre')
    query3_result = cursor.fetchall()
    with open('query3_result.json', 'w', encoding='utf-8') as json_file:
        json.dump(query3_result, json_file, indent=2, ensure_ascii=False)

    cursor.execute('SELECT * FROM books WHERE rating > 2 ORDER BY pages LIMIT %s', (8 + 10,))
    query4_result = cursor.fetchall()
    with open('query4_result.json', 'w', encoding='utf-8') as json_file:
        json.dump(query4_result, json_file, indent=2, default=str, ensure_ascii=False)

# тут я в итоге postgres решил использовать(, он по роднее для меня
conn = psycopg2.connect(
    database="bd_first_task",
    user="postgres",
    password="user",
    host="localhost",
    port="5432"
)

create_table(conn)
populate_table_from_csv(conn)
execute_queries_and_save_to_json(conn)

conn.close()
