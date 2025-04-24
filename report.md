![image](https://github.com/user-attachments/assets/326dc69b-44dd-4e28-900f-0337374eac1b)# LAB 9

1. Đăng nhập vào tài khoảng Github

2. Truy cập vào link
Chọn fork
![image](https://github.com/user-attachments/assets/78bc5f7e-3354-46d3-93ae-37df89da9613)

Click Create fork
![image](https://github.com/user-attachments/assets/bf0a8369-8568-401f-a337-7457d2c25a3b)

## EXERCISE 1

Thực thi lệnh sau trong CMD: git clone để clone GitHub repo về máy của mình
![image](https://github.com/user-attachments/assets/9d638ee5-8343-43e5-b3c9-7f08d1101a2f)

Sau đó tiến hành chạy lệnh `cd data-engineering-practice/Exercises/Exercise-1` để truy cập vào thư mục Exercise-1

Tiếp tục thực hiện lệnh: `docker build --tag=exercise-1` . build Docker image Quá trình sẽ mất vài phút
![image](https://github.com/user-attachments/assets/852153d1-d493-47ea-b267-8a5b6843be07)
![image](https://github.com/user-attachments/assets/ebf1c936-ba6c-4796-8edc-a7ef97b8eaec)
![image](https://github.com/user-attachments/assets/1e3beb59-a0c5-4d13-9f90-209467c30530)

Sử dụng Visual để chạy main.py
![image](https://github.com/user-attachments/assets/2f38e384-09de-4d2d-aa2b-3f0613983592)

Code sử dụng cho main.py
```
import os

import requests

import zipfile

download\_uris = [

    "https://divvy-tripdata.s3.amazonaws.com/Divvy\_Trips\_2018\_Q4.zip",

    "https://divvy-tripdata.s3.amazonaws.com/Divvy\_Trips\_2019\_Q1.zip",

    "https://divvy-tripdata.s3.amazonaws.com/Divvy\_Trips\_2019\_Q2.zip",

    "https://divvy-tripdata.s3.amazonaws.com/Divvy\_Trips\_2019\_Q3.zip",

    "https://divvy-tripdata.s3.amazonaws.com/Divvy\_Trips\_2019\_Q4.zip",

    "https://divvy-tripdata.s3.amazonaws.com/Divvy\_Trips\_2020\_Q1.zip",

    "https://divvy-tripdata.s3.amazonaws.com/Divvy\_Trips\_2220\_Q1.zip",

]

def download\_and\_extract(url, download\_dir):

    filename = url.split("/")[-1]

    zip\_path = os.path.join(download\_dir, filename)

    try:

        print(f"Downloading: {filename}")

        response = requests.get(url, timeout=10)

        response.raise\_for\_status()

        with open(zip\_path, "wb") as f:

            f.write(response.content)

        print(f"Extracting: {filename}")

        with zipfile.ZipFile(zip\_path, 'r') as zip\_ref:

            zip\_ref.extractall(download\_dir)

        os.remove(zip\_path)

        print(f"Finished: {filename}\n")

    except requests.exceptions.HTTPError as http\_err:

        print(f"HTTP error: {http\_err} — Skipping {filename}")

    except zipfile.BadZipFile:

        print(f"Bad zip file: {filename} — Skipping")

    except Exception as e:

        print(f"Unexpected error: {e} — Skipping {filename}")

def main():

    download\_dir = "downloads"

    os.makedirs(download\_dir, exist\_ok=True)

    for url in download\_uris:

        download\_and\_extract(url, download\_dir)

if \_\_name\_\_ == "\_\_main\_\_":

    main()
```
- Tạo thư mục downloads nếu chưa tồn tại

- Tải từng file từ danh sách download\_uris

- Giữ tên gốc của file từ URL

- Giải nén .zip thành .csv

- Xóa file .zip sau khi giải nén

- Bỏ qua URL không hợp lệ (ví dụ: cái Divvy\_Trips\_2220\_Q1.zip không tồn tại)

Sau khi save `main.py`, chạy lệnh `docker-compose up run` (mất khoảng 5 phút)
![image](https://github.com/user-attachments/assets/1937ba2e-ce1e-4977-a496-4767dd5d4ee6)

## EXERCISE 2

Thay đổi thư mục tại CMD thành Exercise-2

Chạy lệnh docker build --tag=exercise-2 . để build image Docker (Quá trình diễn ra trong 2 – 3 phút)
![image](https://github.com/user-attachments/assets/4d41530c-32d7-443c-89e9-27f76d7bb0b7)

Sau khi build xong, truy cập file main.py bằng VS code
![image](https://github.com/user-attachments/assets/bba9a20c-875a-4f40-be67-c7d07b692808)

Nội dung file main.py
```
import requests

from bs4 import BeautifulSoup

import pandas as pd

import os

BASE\_URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"

TARGET\_TIMESTAMP = "2024-01-19 10:27"

def find\_target\_file():

    response = requests.get(BASE\_URL)

    response.raise\_for\_status()

    soup = BeautifulSoup(response.text, 'lxml')

    rows = soup.find\_all("tr")

    for row in rows:

        cols = row.find\_all("td")

        if len(cols) >= 2:

            timestamp = cols[1].text.strip()

            if timestamp == TARGET\_TIMESTAMP:

                filename = cols[0].text.strip()

                return filename

    raise Exception("File with timestamp 2024-01-19 10:27 not found.")

def download\_file(filename):

    download\_url = BASE\_URL + filename

    local\_path = os.path.join("downloads", filename)

    os.makedirs("downloads", exist\_ok=True)

    response = requests.get(download\_url)

    response.raise\_for\_status()

    with open(local\_path, 'wb') as f:

        f.write(response.content)

    print(f"Downloaded file to {local\_path}")

    return local\_path

def analyze\_file(filepath):

    df = pd.read\_csv(filepath)

    if 'HourlyDryBulbTemperature' not in df.columns:

        raise Exception("'HourlyDryBulbTemperature' column not found in the file.")

    # Chuyển đổi nhiệt độ về kiểu số (nếu cần, vì có thể là string)

    df['HourlyDryBulbTemperature'] = pd.to\_numeric(df['HourlyDryBulbTemperature'], errors='coerce')

    

    max\_temp = df['HourlyDryBulbTemperature'].max()

    hottest\_records = df[df['HourlyDryBulbTemperature'] == max\_temp]

    print("\nRecords with the highest HourlyDryBulbTemperature:")

    print(hottest\_records)

def main():

    try:

        print("Looking for file...")

        filename = find\_target\_file()

        print(f"Found file: {filename}")

        filepath = download\_file(filename)

        print("Analyzing file...")

        analyze\_file(filepath)

    except Exception as e:

        print(f"Error: {e}")

if \_\_name\_\_ == "\_\_main\_\_":

    main()
```
Sau khi save file main.py, chạy dòng lệnh `docker-compose up run`

Kết quả thu được
![image](https://github.com/user-attachments/assets/4c307ffa-cb55-4e1d-8213-3fa84081d436)

## EXERCISE 3

Thay đổi thư mục tại CMD thành Exercise-3

Chạy lệnh docker `build --tag=exercise-3` . để build image Docker (Quá trình diễn ra trong 2 – 3 phút)
![image](https://github.com/user-attachments/assets/a7c2b7d6-8b55-44c1-b6da-6766c52e733a)

Sau khi build xong, truy cập file main.py bằng VS code
![image](https://github.com/user-attachments/assets/87407b0d-e131-46fd-a7ab-667784083fb7)

Lỗi module cgi, một thư viện chuẩn của Python dùng để xử lý các script CGI, bị thiếu. Module này là yêu cầu của thư viện botocore, một phụ thuộc của boto3 để làm việc với các dịch vụ AWS. Vì Docker image python:latest đang sử dụng không bao gồm module cgi, do phiên bản Python mới (như Python:latest trong container Docker) có thể không đi kèm với module này.

 Thay đổi docker file thành FROM Python:3.9

## EXERCISE-4

Thay đổi thư mục tại CMD thành Exercise-4

Chạy lệnh docker `build --tag=exercise-4` . để build image Docker (Quá trình diễn ra trong 2 – 3 phút)
![image](https://github.com/user-attachments/assets/db2a2e7d-5dd1-4b13-92f3-b8d3adb230a3)

Nội dung file main.py
```
import os

import json

import csv

import glob

def flatten\_json(nested\_json, parent\_key='', sep='\_'):

    """Giải nén JSON lồng nhau thành cấu trúc phẳng (flat structure)"""

    items = []

    if isinstance(nested\_json, dict):  # Kiểm tra nếu là dictionary

        for k, v in nested\_json.items():

            new\_key = f"{parent\_key}{sep}{k}" if parent\_key else k

            if isinstance(v, dict):  # Nếu giá trị là dict, tiếp tục giải nén

                items.extend(flatten\_json(v, new\_key, sep=sep).items())

            elif isinstance(v, list):  # Nếu giá trị là list, giải nén từng phần tử

                for i, sub\_item in enumerate(v):

                    items.extend(flatten\_json(sub\_item, f"{new\_key}{sep}{i}", sep=sep).items())

            else:  # Nếu giá trị không phải dict hoặc list (ví dụ như số, chuỗi, boolean)

                items.append((new\_key, v))

    elif isinstance(nested\_json, list):  # Kiểm tra nếu là list

        for i, sub\_item in enumerate(nested\_json):

            items.extend(flatten\_json(sub\_item, f"{parent\_key}{sep}{i}", sep=sep).items())

    else:

        # Nếu giá trị là kiểu khác (ví dụ float, int, string), trả về giá trị trực tiếp

        items.append((parent\_key, nested\_json))

    return dict(items)

def convert\_json\_to\_csv(json\_file, csv\_file):

    """Chuyển đổi tệp JSON thành tệp CSV"""

    with open(json\_file, 'r') as f:

        data = json.load(f)

    # Giải nén JSON lồng nhau

    flat\_data = flatten\_json(data)

    # Lưu dữ liệu vào CSV

    with open(csv\_file, 'w', newline='', encoding='utf-8') as f:

        writer = csv.DictWriter(f, fieldnames=flat\_data.keys())

        writer.writeheader()

        writer.writerow(flat\_data)

def process\_json\_files\_in\_directory(data\_directory):

    """Duyệt qua thư mục và chuyển đổi tất cả tệp JSON thành CSV"""

    # Tìm tất cả tệp .json trong thư mục và các thư mục con

    json\_files = glob.glob(os.path.join(data\_directory, '**', '*.json'), recursive=True)

    for json\_file in json\_files:

        csv\_file = json\_file.replace('.json', '.csv')

        convert\_json\_to\_csv(json\_file, csv\_file)

        print(f"Đã chuyển đổi {json\_file} thành {csv\_file}")

def main():

    # Thư mục chứa dữ liệu

    data\_directory = './data'  # Thay đổi đường dẫn nếu cần

    process\_json\_files\_in\_directory(data\_directory)

if \_\_name\_\_ == "\_\_main\_\_":

    main()
```
Sau khi save file main.py, thực thi lệnh docker-compose up run
![image](https://github.com/user-attachments/assets/1c0edd93-1466-414e-873a-e0c941012bfc)
![image](https://github.com/user-attachments/assets/5736c50e-4bd9-46cf-b7e7-9577c53624f5)

EXERCISE-5

Thay đổi thư mục tại CMD thành `Exercise-4`

Chạy lệnh `docker build --tag=exercise-4` . để build image Docker (Quá trình diễn ra trong 2 – 3 phút)
![image](https://github.com/user-attachments/assets/09c296d4-c341-43eb-bb3c-5482fd3b23b8)

Nội dung file main.py:
```
import psycopg2

import csv

# Kết nối đến PostgreSQL

conn = psycopg2.connect(

    dbname="postgres",  # Tên cơ sở dữ liệu

    user="postgres",    # Tên người dùng PostgreSQL

    password="postgres",# Mật khẩu PostgreSQL

    host="postgres",    # Máy chủ (PostgreSQL chạy trên localhost)

    port="5432"         # Cổng PostgreSQL (mặc định)

)

# Tạo một con trỏ để thực thi các câu lệnh SQL

with conn.cursor() as cur:

    # 1. Xóa tất cả các ràng buộc khóa ngoại nếu có

    remove\_constraints = """

    ALTER TABLE IF EXISTS orders DROP CONSTRAINT IF EXISTS orders\_product\_id\_fkey;

    """

    cur.execute(remove\_constraints)

    conn.commit()

    # 2. Xóa các bảng nếu đã tồn tại

    drop\_tables = """

    DROP TABLE IF EXISTS transactions, accounts, products CASCADE;

    """

    cur.execute(drop\_tables)

    conn.commit()

    # 3. Tạo lại các bảng

    create\_products\_table = """

    CREATE TABLE IF NOT EXISTS products (

        product\_id INT PRIMARY KEY,

        product\_code VARCHAR(10),

        product\_description VARCHAR(255)

    );

    """

    

    # Tạo bảng accounts

    create\_accounts\_table = """

    CREATE TABLE IF NOT EXISTS accounts (

        customer\_id INT PRIMARY KEY,

        first\_name VARCHAR(100),

        last\_name VARCHAR(100),

        address\_1 VARCHAR(255),

        address\_2 VARCHAR(255),

        city VARCHAR(100),

        state VARCHAR(50),

        zip\_code VARCHAR(20),

        join\_date DATE

    );

    """

    

    # Tạo bảng transactions (sau khi bảng products đã được tạo)

    create\_transactions\_table = """

    CREATE TABLE IF NOT EXISTS transactions (

        transaction\_id VARCHAR(50) PRIMARY KEY,

        transaction\_date DATE,

        product\_id INT,

        product\_code VARCHAR(10),

        product\_description VARCHAR(255),

        quantity INT,

        account\_id INT

    );

    """

    

    # 4. Chạy các câu lệnh tạo bảng

    print("Creating products table...")

    cur.execute(create\_products\_table)  # Đảm bảo tạo bảng products trước

    print("Creating accounts table...")

    cur.execute(create\_accounts\_table)

    print("Creating transactions table...")

    cur.execute(create\_transactions\_table)

    # Commit changes

    conn.commit()

    # 5. Thêm khóa ngoại sau khi các bảng đã được tạo

    add\_foreign\_keys = """

    ALTER TABLE transactions

    ADD CONSTRAINT fk\_product\_id FOREIGN KEY (product\_id) REFERENCES products(product\_id) ON DELETE CASCADE,

    ADD CONSTRAINT fk\_account\_id FOREIGN KEY (account\_id) REFERENCES accounts(customer\_id) ON DELETE CASCADE;

    """

    print("Adding foreign key constraints...")

    cur.execute(add\_foreign\_keys)

    

    # Commit changes

    conn.commit()

    # 6. Chèn dữ liệu từ CSV vào các bảng

    def load\_data\_from\_csv(csv\_file, table\_name, columns):

        with open(csv\_file, 'r') as file:

            reader = csv.reader(file)

            next(reader)  # Bỏ qua dòng tiêu đề (header row)

            

            for row in reader:

                # Tạo câu truy vấn để chèn dữ liệu

                query = f"INSERT INTO {table\_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"

                cur.execute(query, row)

        

        conn.commit()

    # 7. Chèn dữ liệu cho từng bảng

    load\_data\_from\_csv("data/accounts.csv", "accounts", ["customer\_id", "first\_name", "last\_name", "address\_1", "address\_2", "city", "state", "zip\_code", "join\_date"])

    load\_data\_from\_csv("data/products.csv", "products", ["product\_id", "product\_code", "product\_description"])

    load\_data\_from\_csv("data/transactions.csv", "transactions", ["transaction\_id", "transaction\_date", "product\_id", "product\_code", "product\_description", "quantity", "account\_id"])

# 8. Đóng kết nối

conn.close()

print("Data has been loaded successfully.")
```
Sau khi thực thi lệnh `docker-compose up run`
![image](https://github.com/user-attachments/assets/8493baa6-288d-44fe-b5d5-3a81788ab7ea)
![image](https://github.com/user-attachments/assets/6b683fb4-5738-49ff-b600-79cf8b8762b2)



