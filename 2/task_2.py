import psycopg2
import csv
import msgpack
from datetime import datetime

# Устанавливаем соединение с базой данных PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="bd_first_task",
    user="postgres",
    password="user",
    port="5432"
)

# Создаем курсор для выполнения SQL-запросов
cursor = conn.cursor()

# Создаем таблицу books_prices
cursor.execute("""
CREATE TABLE books_prices (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    price DECIMAL,
    place VARCHAR(255),
    date DATE
)
""")

# Считываем данные из файла task_2_var_08_subitem.msgpack
with open('task_2_var_08_subitem.msgpack', 'rb') as file:
    data = msgpack.unpack(file, raw=False)

# Вставляем данные в таблицу books_prices
for entry in data:
    title = entry['title']
    price = entry['price']
    place = entry['place']
    date = datetime.strptime(entry['date'], '%d.%m.%Y').date()

    cursor.execute("""
    INSERT INTO books_prices (title, price, place, date)
    VALUES (%s, %s, %s, %s)
    """, (title, price, place, date))

# Запрос 1: Выбор заголовков книг из обеих таблиц, где заголовок совпадает
cursor.execute("""
SELECT b.title, bp.title
FROM books b
JOIN books_prices bp ON b.title = bp.title
""")

result_query_1 = cursor.fetchall()

# Записываем результат запроса 1 в CSV
with open('result_query_1.csv', 'w', newline='', encoding="utf-8") as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['Title from books', 'Title from books_prices'])
    csv_writer.writerows(result_query_1)

# Запрос 2: Пример агрегации данных из обеих таблиц по категории (жанру)
cursor.execute("""
SELECT b.genre, AVG(bp.price), MAX(b.rating), COUNT(bp.id)
FROM books b
JOIN books_prices bp ON b.title = bp.title
GROUP BY b.genre
""")

result_query_2 = cursor.fetchall()

# Записываем результат запроса 2 в CSV
with open('result_query_2.csv', 'w', newline='', encoding="utf-8") as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['Genre', 'Average Price', 'Max Rating', 'Count'])
    csv_writer.writerows(result_query_2)

# Запрос 3: Выбор данных, отсортированных по году публикации, из обеих таблиц
cursor.execute("""
SELECT b.title, b.published_year
FROM books b
JOIN books_prices bp ON b.title = bp.title
ORDER BY b.published_year
LIMIT 10
""")

result_query_3 = cursor.fetchall()

with open('result_query_3.csv', 'w', newline='', encoding="utf-8") as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['Title', 'Published Year'])
    csv_writer.writerows(result_query_3)
conn.commit()
cursor.close()
conn.close()
