import io
import json
from datetime import datetime, timedelta, date
import requests
import pandas as pd
from power_fetcher import power_data_fetcher
from water_warning import water_warning
from utils import gsutil_upload
import os
import asyncio
from numpy import nan

base_dir = os.path.dirname(os.path.abspath(__file__))+'/'

covid = "https://raw.github.com/readr-media/readr-data/master/covid-19/indigenous_case_county.csv"
reservoir = "https://storage.googleapis.com/projects.readr.tw/taiwan-dashboard/reservoir.json"
power_peak = "https://raw.githubusercontent.com/readr-media/readr-data/master/electric-power/monthly_peak.csv"
last_year_peak = 3802.01
yesterday = date.today() - timedelta(days=1)
dashboard_output = "dashboard.json"

async def news_fetcher():
    # print("NEWS")
    news = "https://storage.googleapis.com/projects.readr.tw/dashboard_covid_news.json"
    r = requests.get(news)
    news = r.json()
    return news

def df_from_url(url):
    r = requests.get(url).content
    return pd.read_csv(io.StringIO(r.decode('utf-8')))

async def covid_data_fetcher():
    # print("COVID")
    df = df_from_url(covid)

    # 台灣本土總確診
    cases_frame = df_from_url("https://raw.githubusercontent.com/readr-media/readr-data/master/covid-19/covid19_comfirmed_case_taiwan.csv")
    
    city_prev_total = df.iloc[:-1].sum()[1:].to_dict() # 縣市至昨日為止總確診
    city_today = df.iloc[-1][1:-2].to_dict() # 縣市今日新增
    # index[-1]為今日
    # [1:] 去除published
    # [1:-2] 去除published, update_time, back_log

    cases_today = df_from_url("https://raw.githubusercontent.com/readr-media/readr-data/master/covid-19/indigenous_case_group_after0514.csv")

    # Death
    death_total = cases_frame[(cases_frame.case_type=='indigenous case') & (cases_frame.state=='deceased')].shape[0]
    death_today = cases_today.iloc[-1]['death_case']

    for k, v in city_prev_total.items():
        city_prev_total[k] = int(v)
    for k, v in city_today.items():
        city_today[k] = int(v)
    
    city = []
    for k, v in city_today.items():
        data = {"city_name":k, "city_prev_total": city_prev_total[k], "city_today": city_today[k], "level": 3}
        city.append(data)
    
    
    backlog_flag  = df.iloc[-1][-1]
    if backlog_flag is nan:
        backlog_flag = 'n'
    
    return {"today": int(cases_today.iloc[-1]['indigenous case']),
    "city": city,
    "taiwan_total": cases_frame[cases_frame['case_type']=='indigenous case'].shape[0],
    "death_total": death_total,
    "death_today": death_today,
    "backlog": backlog_flag,
    "taiwan_level": 3,
    "update_time": df.iloc[-1][-2]}


async def power_month_peak():
    """Get last month power peak"""
    # print("POWER MONTH PEAK")
    df = df_from_url(power_peak)
    this_month = datetime.now().month
    power_max = df[df.month==this_month].peak.max() # str

    return float(power_max.replace(',',''))/10000

async def power_data_exporter():
    """Read from GCS and pack the data"""
    # print("POWER")
    r = requests.get("https://storage.googleapis.com/projects.readr.tw/power.json")
    power_by_hr = r.json()

    # power_24h_yesterday = [item for item in power_by_hr if (datetime.strptime(item['time'],'%Y-%m-%d %H:%M').date()==yesterday and datetime.strptime(item['time'],'%Y-%m-%d %H:%M').minute==0)]
    # power_24h_today = [item for item in power_by_hr if (datetime.strptime(item['time'],'%Y-%m-%d %H:%M').date()==date.today() and datetime.strptime(item['time'],'%Y-%m-%d %H:%M').minute==0) ]
    power_24h_yesterday = [item for item in power_by_hr if datetime.strptime(item['time'],'%Y-%m-%d %H:%M').date()==yesterday ]
    power_24h_today = [item for item in power_by_hr if datetime.strptime(item['time'],'%Y-%m-%d %H:%M').date()==date.today()  ]

    power_json = {
        "power_24h_yesterday": power_24h_yesterday,
        "power_24h_today":  power_24h_today, # power data within 24hr by hour
        "month_peak" : await power_month_peak(),
        "last_year_peak": last_year_peak,
        "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S") # update time then power_24h has data appended
    }

    return power_json

async def water_data_fetcher():
    # print("WATER")
    r = requests.get(reservoir)
    data = r.json()
    data.update({"warning":water_warning()})
    return data


async def export_data():
    # print("Export DATA")
    data = {"news": await news_fetcher(),
    "water": await water_data_fetcher(),
    "power": await power_data_exporter(),
    "covid": await covid_data_fetcher(),
    "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    return data

async def main():
    data = await export_data()

    with open(base_dir + dashboard_output, 'w') as f:
        f.write(json.dumps(data, ensure_ascii=False).encode('utf8').decode()+'\n')

    gsutil_upload(f"{base_dir}{dashboard_output}")

if __name__ == "__main__":
    # power_data_fetcher()
    asyncio.run(main())
