import msgpack
import csv
import psycopg2
from psycopg2 import sql

# Задайте параметры подключения к базе данных PostgreSQL
db_params = {
    'host': 'localhost',
    'database': 'bd_first_task',
    'user': 'postgres',
    'password': 'user',
}

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

create_table_query = """
CREATE TABLE IF NOT EXISTS music_data (
    id SERIAL PRIMARY KEY,
    artist VARCHAR(255),
    song VARCHAR(255),
    duration_ms INTEGER,
    year INTEGER,
    tempo DECIMAL,
    genre VARCHAR(255),
    energy DECIMAL,
    key INTEGER,
    loudness DECIMAL
);
"""
cursor.execute(create_table_query)
conn.commit()

# Чтение данных из msgpack-файла и внесение их в базу данных
with open('task_3_var_08_part_1.msgpack', 'rb') as file:
    msgpack_data = msgpack.unpack(file, raw=False)

for record in msgpack_data:
    try:
        loudness_value = record.get('loudness', None)
        key_value = record.get('key', None)
        insert_query = sql.SQL("""
            INSERT INTO music_data (artist, song, duration_ms, year, tempo, genre, energy, key, loudness)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """)
        cursor.execute(insert_query, (
            record['artist'], record['song'], record['duration_ms'], record['year'],
            record['tempo'], record['genre'], record.get('energy', None), key_value, loudness_value
        ))
    except KeyError as e:
        print(f"KeyError: {e}")

# Чтение данных из csv-файла и внесение их в базу данных
with open('task_3_var_08_part_2.csv', newline='', encoding='utf-8') as file:
    reader = csv.DictReader(file, delimiter=';')
    for record in reader:
        try:
            loudness_value = record.get('loudness', None)
            key_value = record.get('key', None)
            insert_query = sql.SQL("""
                INSERT INTO music_data (artist, song, duration_ms, year, tempo, genre, energy, key, loudness)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """)
            cursor.execute(insert_query, (
                record['artist'], record['song'], record['duration_ms'], record['year'],
                record['tempo'], record['genre'], record.get('energy', None), key_value, loudness_value
            ))
        except KeyError as e:
            print(f"KeyError: {e}")

# Выполните запросы и сохраните результат в файлы
queries = [
    """
    SELECT * FROM music_data ORDER BY duration_ms LIMIT 8;
    """,
    """
    SELECT
        SUM(duration_ms) as total_duration,
        MIN(duration_ms) as min_duration,
        MAX(duration_ms) as max_duration,
        AVG(duration_ms) as avg_duration
    FROM music_data;
    """,
    """
    SELECT genre, COUNT(*) as frequency FROM music_data GROUP BY genre;
    """,
    """
    SELECT * FROM music_data WHERE year > 2000 ORDER BY tempo LIMIT 23;
    """
]

for i, query in enumerate(queries, 1):
    cursor.execute(query)
    results = cursor.fetchall()
    with open(f'query_result_{i}.json', 'w', encoding="utf-8") as json_file:
        json_file.write("[\n")
        for row in results:
            json_file.write(f"\t{row},\n")
        json_file.write("]\n")

conn.commit()
cursor.close()
conn.close()
