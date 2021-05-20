import io
import json
from datetime import datetime, timedelta, date
import requests
import pandas as pd
from power_fetcher import power_data_fetcher
from water_warning import water_warning
from utils import gsutil_upload


covid = "https://raw.github.com/readr-media/readr-data/master/covid-19/indigenous_case_county.csv"
reservoir = "https://storage.googleapis.com/projects.readr.tw/taiwan-dashboard/reservoir.json"
power_peak = "https://raw.githubusercontent.com/readr-media/readr-data/master/electric-power/monthly_peak.csv"
last_year_peak = 3802.01
yesterday = date.today() - timedelta(days=1)
dashboard_output = "dashboard.json"

def news_fetcher():
    news = "https://storage.googleapis.com/projects.readr.tw/dashboard_covid_news.json"
    r = requests.get(news)
    news = r.json()[-1]
    return news

def covid_data_fetcher():
    r = requests.get(covid).content
    df = pd.read_csv(io.StringIO(r.decode('utf-8')))
    city = df.sum()[1:-1].to_dict()
    for k,v in city.items():
        city[k] = int(v)
    return {"today": int(df.iloc[-1][1:-1].sum()), "city": city, "update_time": df.iloc[-1][0]}


def power_month_peak():
    """Get last month power peak"""
    r = requests.get(power_peak).content
    df = pd.read_csv(io.StringIO(r.decode('utf-8')))
    this_month = datetime.now().month
    power_max = df[df.month==this_month].peak.max() # str

    return float(power_max.replace(',',''))

def power_data_exporter():
    """Read from GCS and pack the data"""
    
    r = requests.get("https://storage.googleapis.com/projects.readr.tw/power.json")
    power_by_hr = r.json()

    power_24h_yesterday = [item for item in power_by_hr if datetime.strptime(item['time'],'%Y-%m-%d %H:%M').date()==yesterday ]
    power_24h_today = [item for item in power_by_hr if datetime.strptime(item['time'],'%Y-%m-%d %H:%M').date()==date.today() ]
    power_json = {
        "power_24h_yesterday": power_24h_yesterday,
        "power_24h_today":  power_24h_today, # power data within 24hr by hour
        "month_peak" : power_month_peak(),
        "last_year_peak": last_year_peak,
        "update_time": datetime.now().strftime("%Y-%m-%d") # update time then power_24h has data appended
    }

    return power_json

def water_data_fetcher():
    r = requests.get(reservoir)
    data = r.json()
    data.update({"warning":water_warning()})
    return data


def export_data():

    data = {"news": news_fetcher(),
    "water": water_data_fetcher(),
    "power": power_data_exporter(),
    "covid": covid_data_fetcher()
    }
    return data

def main():
    data = export_data()

    with open(dashboard_output, 'w') as f:
        f.write(json.dumps(data, ensure_ascii=False).encode('utf8').decode()+'\n')

    gsutil_upload(f"./{dashboard_output}")

if __name__ == "__main__":
    # power_data_fetcher()
    main()