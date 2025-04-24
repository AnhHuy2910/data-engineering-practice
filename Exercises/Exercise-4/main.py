import os
import json
import csv
import glob

def flatten_json(nested_json, parent_key='', sep='_'):
    """Giải nén JSON lồng nhau thành cấu trúc phẳng (flat structure)"""
    items = []
    if isinstance(nested_json, dict):  # Kiểm tra nếu là dictionary
        for k, v in nested_json.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):  # Nếu giá trị là dict, tiếp tục giải nén
                items.extend(flatten_json(v, new_key, sep=sep).items())
            elif isinstance(v, list):  # Nếu giá trị là list, giải nén từng phần tử
                for i, sub_item in enumerate(v):
                    items.extend(flatten_json(sub_item, f"{new_key}{sep}{i}", sep=sep).items())
            else:  # Nếu giá trị không phải dict hoặc list (ví dụ như số, chuỗi, boolean)
                items.append((new_key, v))
    elif isinstance(nested_json, list):  # Kiểm tra nếu là list
        for i, sub_item in enumerate(nested_json):
            items.extend(flatten_json(sub_item, f"{parent_key}{sep}{i}", sep=sep).items())
    else:
        # Nếu giá trị là kiểu khác (ví dụ float, int, string), trả về giá trị trực tiếp
        items.append((parent_key, nested_json))
    return dict(items)

def convert_json_to_csv(json_file, csv_file):
    """Chuyển đổi tệp JSON thành tệp CSV"""
    with open(json_file, 'r') as f:
        data = json.load(f)

    # Giải nén JSON lồng nhau
    flat_data = flatten_json(data)

    # Lưu dữ liệu vào CSV
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=flat_data.keys())
        writer.writeheader()
        writer.writerow(flat_data)

def process_json_files_in_directory(data_directory):
    """Duyệt qua thư mục và chuyển đổi tất cả tệp JSON thành CSV"""
    # Tìm tất cả tệp .json trong thư mục và các thư mục con
    json_files = glob.glob(os.path.join(data_directory, '**', '*.json'), recursive=True)
    for json_file in json_files:
        csv_file = json_file.replace('.json', '.csv')
        convert_json_to_csv(json_file, csv_file)
        print(f"Đã chuyển đổi {json_file} thành {csv_file}")

def main():
    # Thư mục chứa dữ liệu
    data_directory = './data'  # Thay đổi đường dẫn nếu cần
    process_json_files_in_directory(data_directory)

if __name__ == "__main__":
    main()
