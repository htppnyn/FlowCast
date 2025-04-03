import os
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

def get_links(url):
    """ ดึงลิงก์ทั้งหมดจาก URL """
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        return [a['href'] for a in soup.find_all("a", href=True) if not a['href'].startswith("?")]
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching links from {url}: {e}")
        return []

def download_csv(file_url, file_path):
    """ ดาวน์โหลดไฟล์ CSV ถ้ายังไม่มี """
    if not os.path.exists(file_path):
        try:
            r = requests.get(file_url, timeout=15)
            r.raise_for_status()
            with open(file_path, 'wb') as f:
                f.write(r.content)
            print(f"✅ Downloaded: {file_url}")
        except requests.exceptions.RequestException as e:
            print(f"❌ Error downloading {file_url}: {e}")

def fetch_rainfall_data(base_url, download_path):

    os.makedirs(download_path, exist_ok=True)  # ✅ สร้างโฟลเดอร์ที่ถูกต้อง

    """ ดึงข้อมูลทั้งหมดจากเว็บไซต์ """
    year_links = [y for y in get_links(base_url) if y.endswith('/') and y[:-1].isdigit()]
    
    for year in tqdm(year_links, desc="📅 Fetching Years"):
        year_url = os.path.join(base_url, year)
        year_path = os.path.join(download_path, year.strip("/"))
        os.makedirs(year_path, exist_ok=True)

        month_links = get_links(year_url)
        for month in tqdm(month_links, desc=f"📆 {year.strip('/')} - Fetching Months", leave=False):
            month_url = os.path.join(year_url, month)
            month_path = os.path.join(year_path, month.strip("/"))
            os.makedirs(month_path, exist_ok=True)

            file_links = [f for f in get_links(month_url) if f.endswith(".csv")]
            for file in tqdm(file_links, desc=f"📂 {month.strip('/')} - Fetching Files", leave=False):
                file_url = os.path.join(month_url, file)
                file_path = os.path.join(month_path, file)
                download_csv(file_url, file_path)