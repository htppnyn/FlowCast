import pandas as pd

def load_hii_csv(file_path):
    """โหลดข้อมูลน้ำฝนจากไฟล์ CSV ของ HII"""
    df = pd.read_csv(file_path)
    print(f"โหลดข้อมูลสำเร็จ: {len(df)} แถว")
    return df

# ทดสอบโหลดข้อมูล
if __name__ == "__main__":
    df = load_hii_csv("rainfall_data.csv")
    print(df.head())