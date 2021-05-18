_author_ = 'arichland'
import requests
import pymysql
import pydict
import pprint
from datetime import datetime, timedelta, timezone, date
pp = pprint.PrettyPrinter(indent=1)
data = {}

class Main:
    def __init__(self):
        self.dt = datetime
        self.td = timedelta
        self.tz = timezone
        self.strip = self.dt.strptime
        self.strip_format = "%Y-%m-%dT%H:%M:%S"
        self.now = self.dt.now()
        self.utc_sec = self.td(seconds=18000)
        self.sec = self.td(seconds=86400)
        self.ts = self.dt.fromtimestamp

    def update_time(self):
        yd_utc = (self.now + self.utc_sec) - self.sec
        #update_time = self.dt(year=yd_utc.year, month=yd_utc.month, day=yd_utc.day, hour=23, minute=59, second=59)
        update_time = "2019-12-31 23:59:59"
        return update_time

    def call_log(self, time, endpoint, code, descr):
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

    def call(self, method, url, param, limit, endpoint):
        time = self.update_time()
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
            self.call_log(time, endpoint, code, code_descr)
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
                #self.call_log(time, endpoint, code, code_descr)
                records = len(data3)

                calls += 1
                print("   Call:", calls, "| Records:", records, "| Offset:", offset.get("offset"))
                for i in data3:
                    count += 1
                    temp = {count: i}
                    data.update(temp)
        return data

    def inventory(self, type, url, param):
        header = pydict.api_header
        payload = {}
        data1 = requests.request(method=type, url=url, params=param, headers=header, json=payload)
        data2 = data1.json()
        return data2