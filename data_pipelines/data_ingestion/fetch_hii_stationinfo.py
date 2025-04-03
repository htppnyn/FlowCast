import pandas as pd
import psycopg2
from psycopg2 import sql,OperationalError
import os

# ข้อมูลการเชื่อมต่อกับฐานข้อมูล
db_params = {
    "dbname": "flowcast",
    "user": "postgres",
    "password": "flowcast",
    "host": "172.16.0.132",
    "port": "5432"  # หรือ port ที่ใช้งาน
}

# เชื่อมต่อกับ PostgreSQL
try:
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # ตรวจสอบว่าเชื่อมต่อสำเร็จหรือไม่
    print("Connection to PostgreSQL was successful.")

except OperationalError as e:
    print(f"Error: Unable to connect to the database.\n{e}")

# กำหนดพาธของไฟล์ txt ที่ต้องการนำเข้า
file_path = os.path.join(os.getcwd(), 'FlowCast', 'sources', '0station_metadatautf8.txt')
df = pd.read_csv(file_path)

# สร้างคำสั่ง SQL เพื่อบันทึกข้อมูลจาก DataFrame ลงในตาราง PostgreSQL
table_name = 'station_info'

# ตรวจสอบชื่อคอลัมน์ในตาราง PostgreSQL ให้ตรงกับชื่อใน DataFrame
columns = [col for col in df.columns.tolist() if col not in ['id', 'created_at']]

# สร้างคำสั่ง SQL ในการ INSERT ข้อมูล
insert_query = sql.SQL(
    "INSERT INTO {}.{} ({}) VALUES %s"
).format(
    sql.Identifier('flowcast'),  # ชื่อ schema
    sql.Identifier(table_name),  # ชื่อ table
    sql.SQL(', ').join(map(sql.Identifier, columns))  # คอลัมน์ที่ต้องการแทรก
)

# นำข้อมูลจาก DataFrame ไปที่ฐานข้อมูล
# เปลี่ยน DataFrame ให้เป็น list ของ tuple
values = [tuple(row) for row in df[columns].values]

# ใช้ execute_values ในการแทรกข้อมูลทั้งหมดในครั้งเดียว
from psycopg2.extras import execute_values
execute_values(cur, insert_query, values)

# คำสั่ง commit เพื่อบันทึกข้อมูล
conn.commit()

# ปิดการเชื่อมต่อ
cur.close()
conn.close()

print("Data imported successfully!")