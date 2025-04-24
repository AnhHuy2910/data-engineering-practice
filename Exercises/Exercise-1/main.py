import os
import requests
import zipfile
from io import BytesIO

# Danh sách các URL cần tải
download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

# Tạo thư mục downloads nếu chưa tồn tại
if not os.path.exists('downloads'):
    os.makedirs('downloads')

def download_and_extract(url):
    try:
        # Tải file từ URL
        response = requests.get(url)
        response.raise_for_status()  # Kiểm tra nếu có lỗi HTTP
        
        # Lấy tên file từ URL (tên file sẽ là phần cuối của URL)
        filename = url.split('/')[-1]
        
        # Giải nén file zip
        with zipfile.ZipFile(BytesIO(response.content)) as zip_ref:
            # Giải nén tất cả file vào thư mục downloads
            zip_ref.extractall('downloads')
            print(f"✅ Đã giải nén {filename} vào thư mục downloads.")
        
        # Xóa file zip sau khi giải nén
        print(f"✅ Đã xóa file zip {filename}.")
    
    except Exception as e:
        print(f"❌ Lỗi khi tải hoặc giải nén {url}: {e}")

def main():
    # Tải và giải nén từng file trong danh sách download_uris
    for url in download_uris:
        download_and_extract(url)

if __name__ == "__main__":
    main()
