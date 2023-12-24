import csv
import json
import psycopg2

# Рассматриваю крушение титаника (данные взяты с Kaggle соревнования)
# https://www.kaggle.com/competitions/titanic

db_params = {
    'host': 'localhost',
    'database': 'titanic_db',
    'user': 'postgres',
    'password': 'user',
}

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS gender_submission (
        PassengerId INT PRIMARY KEY,
        Survived INT
    );
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS test (
        PassengerId INT PRIMARY KEY,
        Pclass INT,
        Name VARCHAR(255),
        Sex VARCHAR(10),
        Age DECIMAL,
        SibSp INT,
        Parch INT,
        Ticket VARCHAR(20),
        Fare DECIMAL,
        Cabin VARCHAR(20),
        Embarked VARCHAR(1)
    );
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS train (
        PassengerId INT PRIMARY KEY,
        Survived INT,
        Pclass INT,
        Name VARCHAR(255),
        Sex VARCHAR(10),
        Age DECIMAL,
        SibSp INT,
        Parch INT,
        Ticket VARCHAR(20),
        Fare DECIMAL,
        Cabin VARCHAR(20),
        Embarked VARCHAR(1)
    );
""")

# Вставка данных
with open('gender_submission.csv', newline='') as file:
    reader = csv.reader(file)
    next(reader)
    for row in reader:
        cursor.execute("INSERT INTO gender_submission (PassengerId, Survived) VALUES (%s, %s)", (row[0], row[1]))

with open('test.csv', newline='') as file:
    reader = csv.reader(file)
    next(reader)
    for row in reader:
        row = [value if value != '' else None for value in row]

        cursor.execute("""
            INSERT INTO test (PassengerId, Pclass, Name, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]))

with open('train.csv', newline='') as file:
    reader = csv.reader(file)
    next(reader)
    for row in reader:
        # Обработка пустых значений для числовых полей
        row = [value if value != '' else None for value in row]

        cursor.execute("""
            INSERT INTO train (PassengerId, Survived, Pclass, Name, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11]))

conn.commit()

# Запросы
queries = [
    """
    SELECT * FROM train WHERE Survived = 1 ORDER BY Age LIMIT 10;
    """,
    """
    SELECT
        Sex,
        AVG(Age) as avg_age,
        MAX(Fare) as max_fare
    FROM train
    GROUP BY Sex;
    """,
    """
    SELECT
        Pclass,
        COUNT(*) as passenger_count
    FROM train
    GROUP BY Pclass;
    """,
    """
    SELECT * FROM train WHERE Age IS NULL LIMIT 5;
    """,
    """
    SELECT
        Survived,
        Sex,
        COUNT(*) as count
    FROM train
    GROUP BY Survived, Sex
    ORDER BY Survived, Sex;
    """,
    """
    SELECT * FROM test
    WHERE Pclass = 1
    ORDER BY Age DESC
    LIMIT 5;
    """,
    """
    SELECT
        Survived,
        COUNT(*) as passenger_count
    FROM train
    GROUP BY Survived;
    """
]

# Сохранение результатов в JSON-файлы
for i, query in enumerate(queries, 1):
    cursor.execute(query)
    results = cursor.fetchall()

    with open(f'result_query_{i}.json', 'w', encoding='utf-8') as json_file:
        json.dump(results, json_file, default=str, ensure_ascii=False)

conn.commit()
cursor.close()
conn.close()
