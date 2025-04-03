import os
from data_ingestion.fetch_hii_data import fetch_rainfall_data
from data_storage.save_to_postgres import process_csv_files

# 🌍 URL ของ API หรือไฟล์ที่ต้องการดาวน์โหลด
BASE_URL = "https://tiservice.hii.or.th/opendata/data_catalog/hourly_rain/"
DOWNLOAD_PATH = "./rain_data"

def main():
    """รันทั้ง pipeline"""
    print("🚀 Starting Data Pipeline")
    
    # 📥 1. ดาวน์โหลดข้อมูลฝน
    fetch_rainfall_data(BASE_URL, DOWNLOAD_PATH)
    
    # 🏗️ 2. ประมวลผล CSV และบันทึกลง PostgreSQL
    process_csv_files()
    
    print("✅ Data Pipeline Completed")

if __name__ == "__main__":
    main()
