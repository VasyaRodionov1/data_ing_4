import json
import csv
import psycopg2
from psycopg2 import sql

# Параметры подключения к базе данных PostgreSQL
db_params = {
    'host': 'localhost',
    'database': 'bd_first_task',
    'user': 'postgres',
    'password': 'user',
}

def find_product_id(cursor, product_name):
    cursor.execute("""
        SELECT id FROM products WHERE LOWER(name) = LOWER(%s) LIMIT 1
    """, (product_name,))
    result = cursor.fetchone()
    return result[0] if result else None

def update_product(cursor, product_id, update_method, update_param):
    cursor.execute("""
        INSERT INTO updates (product_id, update_method, update_param)
        VALUES (%s, %s, %s)
    """, (product_id, update_method, update_param))

    if update_method == 'price_abs':
        cursor.execute("""
            UPDATE products SET price = price + %s WHERE id = %s
        """, (update_param, product_id))
    elif update_method == 'quantity_add':
        cursor.execute("""
            UPDATE products SET quantity = quantity + %s WHERE id = %s
        """, (update_param, product_id))
    elif update_method == 'quantity_sub':
        cursor.execute("""
            UPDATE products SET quantity = quantity - %s WHERE id = %s
        """, (update_param, product_id))
    elif update_method == 'available':
        cursor.execute("""
            UPDATE products SET is_available = %s::BOOLEAN WHERE id = %s
        """, (update_param, product_id))

def process_updates(cursor, updates):
    for update in updates:
        product_name = update['name']
        update_method = update['method']
        update_param = update['param']

        product_id = find_product_id(cursor, product_name)

        if product_id is not None:
            update_product(cursor, product_id, update_method, update_param)
        else:
            print(f"Product with name '{product_name}' not found.")

# Подключение к базе данных
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Создание таблиц
create_table_queries = [
    """
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        price DECIMAL,
        quantity INTEGER,
        from_city VARCHAR(255),
        is_available BOOLEAN,
        views INTEGER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS updates (
        id SERIAL PRIMARY KEY,
        product_id INTEGER REFERENCES products(id),
        update_method VARCHAR(50),
        update_param VARCHAR(255)
    );
    """
]

for query in create_table_queries:
    cursor.execute(query)

# Загрузка данных из JSON-файла и внесение их в таблицу products
with open('task_4_var_08_product_data.json', 'r', encoding='utf-8') as file:
    products_data = json.load(file)

for product in products_data:
    insert_query = sql.SQL("""
        INSERT INTO products (name, price, quantity, from_city, is_available, views)
        VALUES (%s, %s, %s, %s, %s, %s)
    """)
    cursor.execute(insert_query, (
        product.get('name'),
        product.get('price'),
        product.get('quantity'),
        product.get('fromCity'),
        product.get('isAvailable'),
        product.get('views')
    ))

# Загрузка данных из CSV-файла и внесение их в таблицу updates
with open('task_4_var_08_update_data.csv', newline='', encoding='utf-8') as file:
    reader = csv.DictReader(file, delimiter=';')
    updates = list(reader)

process_updates(cursor, updates)

# Выполнение запросов
queries = [
    """
    SELECT * FROM products ORDER BY views DESC LIMIT 10;
    """,
    """
    SELECT
        AVG(price) as avg_price,
        MIN(price) as min_price,
        MAX(price) as max_price,
        COUNT(*) as product_count
    FROM products
    GROUP BY quantity;
    """,
    """
    SELECT
        SUM(quantity) as sum_quantity,
        MIN(quantity) as min_quantity,
        MAX(quantity) as max_quantity,
        AVG(quantity) as avg_quantity
    FROM products;
    """
]

for i, query in enumerate(queries, 1):
    cursor.execute(query)
    results = cursor.fetchall()

    with open(f'result_query_{i}.json', 'w', encoding='utf-8') as json_file:
        json.dump(results, json_file, default=str, ensure_ascii=False)

# Произвольный запрос
arbitrary_query = """
    SELECT 
        name,
        SUM(quantity * price) as total_value
    FROM products
    GROUP BY name
    HAVING SUM(quantity * price) > 1000;
"""
cursor.execute(arbitrary_query)
arbitrary_result = cursor.fetchall()
with open('proizvol_query_result.json', 'w', encoding='utf-8') as json_file:
    json.dump(arbitrary_result, json_file, default=str, ensure_ascii=False)

conn.commit()
cursor.close()
conn.close()
