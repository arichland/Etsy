_author_ = 'arichland'
import requests
import pymysql
import pydict
import pprint
from datetime import datetime, timedelta, timezone, date
pp = pprint.PrettyPrinter(indent=1)
data = {}

# Setup Epoch timestamp for Etsy API
dt = datetime
td = timedelta
tz = timezone
strip = dt.strptime
strip_format = "%Y-%m-%dT%H:%M:%S"
now = dt.now()
utc_sec = td(seconds=18000)
sec = td(seconds=86400)
ts = dt.fromtimestamp

class api:
    def update_time():
        utc_sec = td(seconds=18000)
        yd_sec = td(seconds=86400)
        yd_utc = (now + utc_sec) - yd_sec

        update_time = dt(year=yd_utc.year, month=yd_utc.month, day=yd_utc.day, hour=23, minute=59, second=59)
        #update_time = "2020-03-01 23:59:59"
        return update_time

    def call_log(time, endpoint, code, descr):
        user = pydict.sql_dict.get('user')
        password = pydict.sql_dict.get('password')
        host = pydict.sql_dict.get('host')
        charset = pydict.sql_dict.get('charset')
        db = "rtcaws01"
        con = pymysql.connect(user=user, password=password, host=host, database=db, charset=charset)

        with con.cursor() as cur:
            qry_update = """INSERT INTO tbl_api_log(timestamp, epoch, market, endpoint, api_code, code_descr) VALUES(NOW(), %s, %s, %s, %s, %s);"""
            market = "Etsy"
            cur.execute(qry_update, (time, market, endpoint, code, descr))
            con.commit()
        cur.close()
        con.close()

    def call_etsy(method, url, param, limit, endpoint):
        time = api.update_time()
        calls = 0
        count = 0
        header = pydict.api_header
        payload = {}
        data1 = requests.request(method=method, url=url, params=param, headers=header, json=payload)
        data2 = data1.json()
        data3 = data2['results']
        code = data1.status_code
        code_descr = pydict.api_codes.get("etsy").get(code)
        records = len(data3)

        if records < limit:
            api.call_log(time, endpoint, code, code_descr)
            for i in data3:
                count += 1
                temp = {count: i}
                data.update(temp)
        else:
            while records == limit:
                offset = {"offset": calls * limit}
                param.update(offset)
                data1 = requests.get(url, params=param, headers=header, json=payload)
                data2 = data1.json()
                data3 = data2['results']
                code = data1.status_code
                code_descr = pydict.api_codes.get("etsy").get(code)
                api.call_log(time, endpoint, code, code_descr)
                records = len(data3)

                calls += 1
                print("   Call:", calls, "| Records:", records, "| Offset:", offset.get("offset"))
                for i in data3:
                    count += 1
                    temp = {count: i}
                    data.update(temp)
        return data

    def inventory(type, url, param):
        header = pydict.api_header
        payload = {}
        data1 = requests.request(method=type, url=url, params=param, headers=header, json=payload)
        data2 = data1.json()
        return data2