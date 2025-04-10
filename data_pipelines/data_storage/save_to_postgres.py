import os
import pandas as pd
import psycopg2
from tqdm import tqdm

# 🔧 ตั้งค่า Database Connection
DB_CONFIG = {
    "dbname": "flowcast",
    "user": "postgres",
    "password": "flowcast",
    "host": "172.16.0.132",
    "port": "5432"
}

CSV_DIR = "./rain_data"

def connect_db():
    """เชื่อมต่อ PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Connected to PostgreSQL")
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def insert_data(df, station_code, conn):
    """Insert ข้อมูล CSV เข้า PostgreSQL"""
    cursor = conn.cursor()
    query = """
    INSERT INTO flowcast.rainfalltimeseries (station_code, time, rainfall, created_at)
    VALUES (%s, %s, %s, NOW())
    ON CONFLICT DO NOTHING;
    """
    
    df = df.dropna(subset=['timestamp', 'rainfall'])  # ลบ NaT และ None
    df["rainfall"] = df["rainfall"].replace([-999, '-'], None)
    data = [(station_code, row['timestamp'], row['rainfall']) for _, row in df.iterrows()]
    
    try:
        cursor.executemany(query, data)
        conn.commit()
        print(f"✅ Inserted: {station_code}")
    except Exception as e:
        conn.rollback()
        print(f"❌ Error inserting data for {station_code}: {e}")
    finally:
        cursor.close()

def delete_file_and_empty_folders(file_path):
    """ลบไฟล์ CSV และลบโฟลเดอร์ที่ว่าง"""
    try:
        os.remove(file_path)  # ลบไฟล์ CSV
        print(f"🗑️ Deleted: {file_path}")

        # 🔄 ไล่ลบโฟลเดอร์ที่ว่าง
        folder = os.path.dirname(file_path)
        while folder != CSV_DIR and not os.listdir(folder):
            os.rmdir(folder)
            print(f"Deleted empty folder: {folder}")
            folder = os.path.dirname(folder)  # ขยับขึ้นไปยังโฟลเดอร์บน
    except Exception as e:
        print(f"Failed to delete {file_path}: {e}")

def process_csv_files():
    """อ่านไฟล์ CSV และบันทึกลง PostgreSQL"""
    conn = connect_db()
    if not conn:
        print("❌ Failed to connect to database.")
        return

    try:
        for year in tqdm(os.listdir(CSV_DIR), desc="📅 Processing Years"):
            year_path = os.path.join(CSV_DIR, year)
            if not os.path.isdir(year_path):
                continue

            for month in tqdm(os.listdir(year_path), desc=f"📆 {year} Processing Months", leave=False):
                month_path = os.path.join(year_path, month)
                if not os.path.isdir(month_path):
                    continue

                for file in tqdm(os.listdir(month_path), desc=f"📂 {month} Processing Files", leave=False):
                    if file.endswith(".csv"):
                        file_path = os.path.join(month_path, file)

                        # 📌 Extract station_code from filename
                        station_code = file.split(".")[0]  # สมมติชื่อไฟล์เป็น "rain_123.csv"

                        try:
                            # Read the CSV file
                            df = pd.read_csv(file_path)

                            # Check the number of columns
                            if df.shape[1] == 3:
                                df.columns = ["date", "time", "rainfall"]
                                # Combine date and time into a timestamp
                                df["timestamp"] = pd.to_datetime(df["date"] + " " + df["time"], format="%d/%m/%Y %H:%M", errors='coerce')
                            elif df.shape[1] == 4:
                                df.columns = ["station_code", "measure_datetime", "rainfall_1h", "quality_flag"]
                                # Convert 'measure_datetime' to timestamp
                                df["timestamp"] = pd.to_datetime(df["measure_datetime"], format='%Y-%m-%d %H:%M:%S', errors='coerce')

                                # Convert to desired format only when needed
                                df["rainfall"] = pd.to_numeric(df["rainfall_1h"], errors='coerce')  # Convert rainfall to numeric

                            else:
                                print(f"⚠️ Warning: File {file} has unexpected column count: {df.shape[1]}")
                                continue  # Skip files with unexpected column count

                            # Clean up and filter the dataframe
                            df = df.dropna(subset=["timestamp", "rainfall"])

                            # Only keep necessary columns
                            df = df[["timestamp", "rainfall"]]  # Keep only timestamp and rainfall columns

                            # Insert data into the database
                            insert_data(df, station_code, conn)

                            # ✅ ลบไฟล์หลังจาก insert สำเร็จ
                            # delete_file_and_empty_folders(file_path)

                        except Exception as e:
                            print(f"❌ Error processing {file}: {e}")

    except Exception as e:
        print(f"❌ Error in the overall process: {e}")
    finally:
        if conn:
            conn.close()  # Ensure the connection is closed after processing
