import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import sys

# URL của thư mục chứa dữ liệu
URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
# Thời điểm cần tìm
TARGET_TIMESTAMP = "2024-01-19 10:27"

def scrape_file_name():
    """
    Scrape bảng <table> trên trang để tìm file .csv
    có Last Modified = TARGET_TIMESTAMP.
    """
    print(f"🔍 Fetching directory listing from {URL}")
    r = requests.get(URL)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if not table:
        print("❌ Không tìm thấy bảng <table> chứa danh sách file.")
        return None

    # Duyệt qua từng hàng (bỏ header)
    rows = table.find_all("tr")[1:]
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 2:
            continue
        last_mod = cols[1].text.strip()
        if TARGET_TIMESTAMP in last_mod:
            link = cols[0].find("a", href=True)
            if link and link["href"].endswith(".csv"):
                filename = link["href"]
                print(f"✅ Matched: {filename}  ({last_mod})")
                return filename

    print("❌ Không tìm thấy file phù hợp với timestamp.")
    return None

def download_file(filename):
    """
    Tải về và lưu vào thư mục downloads/.
    """
    file_url = URL + filename
    print(f"⬇️ Downloading {file_url}")
    r = requests.get(file_url)
    r.raise_for_status()

    os.makedirs("downloads", exist_ok=True)
    local_path = os.path.join("downloads", filename)
    with open(local_path, "wb") as f:
        f.write(r.content)
    print(f"✅ Saved to {local_path}")
    return local_path

def analyze_temperature(csv_path):
    """
    Đọc CSV, tìm bản ghi có HourlyDryBulbTemperature cao nhất,
    và in gọn 3 cột: STATION, DATE, HourlyDryBulbTemperature.
    """
    print(f"🔎 Analyzing {csv_path}")
    df = pd.read_csv(csv_path)

    col = "HourlyDryBulbTemperature"
    if col not in df.columns:
        print(f"❌ Column '{col}' không tồn tại. Các cột có sẵn:")
        print(df.columns.tolist())
        return

    # Chuyển giá trị sang số, bỏ giá trị lỗi
    df[col] = pd.to_numeric(df[col], errors="coerce")
    max_temp = df[col].max()
    top = df[df[col] == max_temp]

    print(f"🌡️ Highest {col} = {max_temp}")
    print(top[["STATION","DATE",col]].to_string(index=False))

def main():
    print("=== Exercise 2: Web scrape + download + Pandas analysis ===")
    filename = scrape_file_name()
    if not filename:
        sys.exit(1)

    csv_path = download_file(filename)
    analyze_temperature(csv_path)

if __name__ == "__main__":
    main()
