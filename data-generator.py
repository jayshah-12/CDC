import mysql.connector as mysql
from faker import Faker
import random

# Database connection details
HOSTNAME = 'localhost'
DATABASE = 'my_db'
USERNAME = 'root'
PASSWORD = 'test'
PORT = 3308  # Ensure PORT is an integer

# Establish the database connection
try:
    conn = mysql.connect(
        host=HOSTNAME,
        user=USERNAME,
        password=PASSWORD,
        database=DATABASE,
        port=PORT
    )
    conn.autocommit = True
except mysql.Error as err:
    print(f"Error connecting to the database: {err}")
    exit(1)

def read_schema(table_name):
    cur = conn.cursor()
    cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'")
    table_schema = cur.fetchall()
    cur.close()
    return table_schema

def get_max_id(table_name):
    cur = conn.cursor()
    cur.execute(f"SELECT MAX(id) FROM {table_name}")
    max_id = cur.fetchone()[0]
    cur.close()
    return max_id if max_id is not None else 0

def create_random_data(table_schema, next_id):
    fake = Faker()
    random_data = []
    for column in table_schema:
        column_name, data_type = column
        if column_name == 'id':
            random_data.append(next_id)
        elif 'varchar' in data_type:
            random_data.append(fake.name())
        elif 'int' in data_type:
            random_data.append(random.randint(1, 100))
        else:
            random_data.append(fake.text())  # For unexpected types
    return random_data

def insert_data(table_name, num_records):
    table_schema = read_schema(table_name)
    col_names = [x[0] for x in table_schema]
    col_names_str = ', '.join(col_names)

    next_id = get_max_id(table_name) + 1
    
    for _ in range(num_records):
        random_data = create_random_data(table_schema, next_id)
        next_id += 1
        cur = conn.cursor()
        values_str = ', '.join(['%s'] * len(random_data))
        insert_query = f"INSERT INTO {table_name} ({col_names_str}) VALUES ({values_str})"
        try:
            cur.execute(insert_query, random_data)
            conn.commit()
            print(f"Inserted: {random_data}")
        except mysql.Error as err:
            print(f"Error inserting data: {err}")
        finally:
            cur.close()

def fetch_data(table_name):
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name}")
    data = cur.fetchall()
    cur.close()
    return data

# Example usage
try:
    insert_data("test", 50)
    data = fetch_data("test")
    for row in data:
        print(row)
except Exception as e:
    print(f"Error during database operations: {e}")
finally:
    conn.close()
