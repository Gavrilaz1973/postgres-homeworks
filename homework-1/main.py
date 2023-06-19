import csv
import os
import psycopg2


password = str(os.getenv('PASSWORD_PGADMIN'))

rows_customers = []
with open('/home/gavrilaz/PycharmProjects/postgres-homeworks/homework-1/north_data/customers_data.csv', 'r',
          encoding='windows-1251') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        rows_customers.append([row['customer_id'], row['company_name'], row['contact_name']])

conn = psycopg2.connect(host="localhost", database="north", user="postgres", password=password)

try:
    with conn:
        with conn.cursor() as cur:
            for row in rows_customers:
                cur.execute("INSERT INTO customers VALUES (%s, %s, %s)", row)
finally:
    conn.close()

rows_employees = []
with open('/home/gavrilaz/PycharmProjects/postgres-homeworks/homework-1/north_data/employees_data.csv', 'r',
          encoding='windows-1251') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        rows_employees.append([row['employee_id'], row['first_name'], row['last_name'], row['title'], row['birth_date'], row['notes']])

conn = psycopg2.connect(host="localhost", database="north", user="postgres", password=password)

try:
    with conn:
        with conn.cursor() as cur:
            for row in rows_employees:
                cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", row)
finally:
    conn.close()


rows = []
with open('/home/gavrilaz/PycharmProjects/postgres-homeworks/homework-1/north_data/orders_data.csv', 'r',
          encoding='windows-1251') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        rows.append([row['order_id'], row['customer_id'], row['employee_id'], row['order_date'], row['ship_city']])

conn = psycopg2.connect(host="localhost", database="north", user="postgres", password=password)

try:
    with conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", row)
finally:
    conn.close()

