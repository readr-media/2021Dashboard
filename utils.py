import os

def gsutil_upload(output_file_name):
    os.system(f'gsutil -m -h "Content-Type:application/json" -h "Cache-Control:max-age=600,public" cp -z gzip -a public-read -r {output_file_name}  gs://projects.readr.tw/')
    print(f"https://storage.googleapis.com/projects.readr.tw/{output_file_name}")