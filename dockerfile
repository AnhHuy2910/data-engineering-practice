FROM python:3.10-slim

# Cài đặt các thư viện cần thiết
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép mã nguồn từ thư mục hiện tại vào trong container
COPY . /app

WORKDIR /app

# Lệnh chạy khi container được khởi động
CMD ["python", "main.py"]
