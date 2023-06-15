import csv

import psycopg2

rows = []
with open('/home/gavrilaz/PycharmProjects/postgres-homeworks/homework-1/north_data/orders_data.csv', 'r',
          encoding='windows-1251') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        rows.append([row['order_id'], row['customer_id'], row['employee_id'], row['order_date'], row['ship_city']])

conn = psycopg2.connect(host="localhost", database="north", user="postgres", password="1973")

try:
    with conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", row)
            cur.execute("SELECT * FROM orders;")
            rows_all = cur.fetchall()
            for row in rows_all:
                print(row)
finally:
    conn.close()


