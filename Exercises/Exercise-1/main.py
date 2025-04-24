import os
import requests
import zipfile

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def download_and_extract(url, download_dir):
    filename = url.split("/")[-1]
    zip_path = os.path.join(download_dir, filename)

    try:
        print(f"Downloading: {filename}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        with open(zip_path, "wb") as f:
            f.write(response.content)

        print(f"Extracting: {filename}")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(download_dir)

        os.remove(zip_path)
        print(f"Finished: {filename}\n")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error: {http_err} — Skipping {filename}")
    except zipfile.BadZipFile:
        print(f"Bad zip file: {filename} — Skipping")
    except Exception as e:
        print(f"Unexpected error: {e} — Skipping {filename}")

def main():
    download_dir = "downloads"
    os.makedirs(download_dir, exist_ok=True)

    for url in download_uris:
        download_and_extract(url, download_dir)

if __name__ == "__main__":
    main()
