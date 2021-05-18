import io
import json
from datetime import datetime
import requests
from requests_html import HTMLSession
import pandas as pd

NEWS = "https://storage.googleapis.com/projects.readr.tw/dashboard_covid_news.json"
covid = "https://raw.github.com/readr-media/readr-data/master/covid-19/indigenous_case_county.csv"
last_year_peak = 3802.01

def power_data_fetcher():
    # generated
    r = requests.get("http://data.taipower.com.tw/opendata01/apply/file/d006001/001.txt")
    r.content.decode('utf-8')
    gen = json.loads(r.content.decode('utf-8'))
    gg = []
    for item in gen['aaData']:
        if not item[3].endswith('%)') and item[3]!='N/A':
            try:
                c = float(item[3])
                gg.append(c)
            except ValueError:
                continue
    generated = sum(gg)
    update_time = gen['']

    #consumed
    session = HTMLSession()
    r = session.get("https://www.taipower.com.tw/d006/loadGraph/loadGraph/load_briefing3.html") # yearly
    r.html.render()
    c = r.html.xpath('//*[@id="latest_load"]/text()')[0] # string
    consumed = float(c.replace(',',''))
    data = {"time": update_time, "status": {"發電": generated, "用電": consumed}}

    with open("./power.json", 'a') as f:
        f.write(json.dumps(data, ensure_ascii=False).encode('utf8').decode()+'\n')

    return generated, consumed, update_time


def news_fetcher():
    r = requests.get(NEWS)
    news = r.json()[-1]
    news["title"] = news.pop("標題")
    news["update_time"] = news.pop("更新時間")
    news["author"] = news.pop("記者")
    news["content"] = news.pop("內文")
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
        "update_time": datetime.now() # update time then power_24h has data appended
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
    "power": power_data_fetcher(),
    "covid": covid_data_fetcher()
    }
    return data

def main():
    data = export_data()

    with open("./dashboard.json", 'a') as f:
        f.write(json.dumps(data, ensure_ascii=False).encode('utf8').decode()+'\n')


if __name__ == "__main__":
    # power_data_fetcher()
    main()