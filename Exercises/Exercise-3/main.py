import io
import gzip
import requests
from dotenv import load_dotenv

load_dotenv()
def download_file_from_url(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    else:
        print(f"Error downloading file: {response.status_code}")
        return None
def main():
    url = 'https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-05/wet.paths.gz'
    gz_content = download_file_from_url(url)
    
    if gz_content:
        with gzip.GzipFile(fileobj=io.BytesIO(gz_content)) as f:
            first_line = f.readline().decode('utf-8').strip() 
            print(f"First line from wet.paths.gz: {first_line}")
            uri = first_line
            print(f"Extracted URI: {uri}")
            print("\nPrinting the first 50 lines from wet.paths.gz:")
            for i, line in enumerate(f):
                if i >= 50:  
                    break
                print(line.decode('utf-8').strip()) 
if __name__ == "__main__":
    main()
