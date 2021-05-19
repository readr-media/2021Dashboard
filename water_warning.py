import requests
from lxml.etree import HTML

def water_warning():
    water = "https://www.wra.gov.tw/EarlyWarning.aspx?n=18804&sms=0"
    r = requests.get(water)
    wh = HTML(r.text)
    raw_warning = wh.xpath('//*[@id="CCMS_Content"]/div/div/div/div[2]/div/div[2]/div/div/div[2]/div[3]/ul/li/div/text()')

    warning = []
    date = []
    location = []
    status = []
    for i, item in enumerate(raw_warning):
        
        if i%3==0: 
            date.append(item) 
        elif i%3==1:
            location.append(item)
        else:
            status.append(item)
    for i, _ in enumerate(date):
        s = {"date":date[i], "location": location[i], "status":status[i]}
        warning.append(s)
    
    return warning