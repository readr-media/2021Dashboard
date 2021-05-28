import os, io
import requests
import pandas as pd

def df_from_url(url, **kwargs):
    header = kwargs.get("header")
    r = requests.get(url).content
    if header:
        return pd.read_csv(io.StringIO(r.decode('utf-8')))
    else:
        return pd.read_csv(io.StringIO(r.decode('utf-8')), header=header)


def gsutil_upload(output_file):
    base_dir, filename = os.path.split(output_file)
    os.system(f'gsutil -m -h "Content-Type:application/json" -h "Cache-Control:max-age=320,public" cp -z gzip -a public-read -r {output_file}  gs://projects.readr.tw/')
    print(f"https://storage.googleapis.com/projects.readr.tw/{filename}")