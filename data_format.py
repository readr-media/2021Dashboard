import io
import json
from datetime import datetime
import requests
import pandas as pd
from power_fetcher import power_data_fetcher
from utils import gsutil_upload

NEWS = "https://storage.googleapis.com/projects.readr.tw/dashboard_covid_news.json"
covid = "https://raw.github.com/readr-media/readr-data/master/covid-19/indigenous_case_county.csv"
last_year_peak = 3802.01

file_name = "dashboard.json"

def news_fetcher():
    r = requests.get(NEWS)
    news = r.json()[-1]
    # news["title"] = news.pop("標題")
    # news["update_time"] = news.pop("更新時間")
    # news["author"] = news.pop("記者")
    # news["content"] = news.pop("內文")
    return news

def covid_data_fetcher():
    r = requests.get(covid).content
    df = pd.read_csv(io.StringIO(r.decode('utf-8')))
    city = df.sum()[1:-1].to_dict()
    for k,v in city.items():
        city[k] = int(v)
    return {"today": int(df.iloc[-1][1:-1].sum()), "city": city, "update_time": df.iloc[-1][0]}

def power_data_exporter():
    
    with open("./power.json") as f:
        power_by_hr = [ json.loads(line) for line in f.readlines()] # parse \n ?

    power_json = {
        "power_24h":  power_by_hr, # power data within 24hr by hour
        # "month_peak" : month_peak,
        "last_year_peak": last_year_peak,
        "update_time": datetime.now().strftime("%Y-%m-%d") # update time then power_24h has data appended
    }

    return power_json

def water_data_fetcher():
    water = {
            "N":[],
            "C":[],
            "S":[],
            "E":[],
        "update_time": datetime.now()
    }
    return water


def export_data():

    data = {"news": news_fetcher(),
    # "water": water_data_fetcher(),
    "power": power_data_exporter(),
    "covid": covid_data_fetcher()
    }
    return data

def main():
    data = export_data()

    with open(file_name, 'w') as f:
        f.write(json.dumps(data, ensure_ascii=False).encode('utf8').decode()+'\n')

    gsutil_upload(f"./{file_name}")

if __name__ == "__main__":
    # power_data_fetcher()
    main()