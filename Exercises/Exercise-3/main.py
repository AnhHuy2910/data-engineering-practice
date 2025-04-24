import requests
import gzip
import io

# URL của tệp WET từ Common Crawl
wet_file_url = 'https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-05/wet.paths.gz'

# Tải tệp WET từ URL
response = requests.get(wet_file_url)

if response.status_code == 200:
    print("Đã tải tệp WET thành công!")
else:
    print(f"Lỗi khi tải tệp: {response.status_code}")
    exit()

# Giải nén và đọc tệp WET
with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
    count = 0  # Biến đếm số dòng đã in
    while count < 50:
        line = gz.readline()
        if not line:
            break
        print(line.decode("utf-8").strip())
        count += 1
