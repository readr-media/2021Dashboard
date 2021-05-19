from requests_html import HTMLSession
import requests
import json
from utils import gsutil_upload
file_name = "power.json"

monthly_peak = "https://raw.githubusercontent.com/readr-media/readr-data/master/electric-power/monthly_peak.csv"

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

    with open(file_name, 'a') as f:
        f.write(json.dumps(data, ensure_ascii=False).encode('utf8').decode()+'\n')

    gsutil_upload(f'./{file_name}')
    return generated, consumed, update_time

if __name__ == "__main__":
    power_data_fetcher()