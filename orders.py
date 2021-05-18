_author_ = 'arichland'
import sql
import pydict
import pprint
from datetime import datetime, timedelta, timezone, date
import api
pp = pprint.PrettyPrinter(indent=1)

class Main:
    def __init__(self):
        self.dt = datetime
        self.td = timedelta
        self.tz = timezone
        self.strip = self.dt.strptime
        self.strip_format = "%Y-%m-%dT%H:%M:%S"
        self.now = self.dt.now()
        self.ts = self.dt.fromtimestamp
        self.api_object = api.Main()
        self.sql_object = sql.OrderQuery()
        self.time = self.api_object.update_time()
        self.limit = 100
        self.offset = 0
        self.url = pydict.etsy_urls

    def new_receipts(self):
        print("Retrieving New Etsy Receipts Data")
        url = self.url.get('receipts')
        params = {"limit": self.limit,
                  "offset": self.offset,
                  "min_created": self.time}
        endpoint = "New Receipts"
        data = self.api_object.call("Get", url, params, self.limit, endpoint)
        print("   Etsy Receipts API Call Complete")
        self.sql_object.new_receipts(data)
        #pp.pprint(data)
        print("New Etsy Receipts Data Process Complete\n")

    def update_receipts(self):
        print("Retrieving Update Etsy Receipts Data")
        url = self.url.get('receipts')
        params = {"limit": self.limit,
                  "offset": self.offset,
                  "min_last_modified": self.time}
        endpoint = "Update Receipts"
        method = "Get"
        data = self.api_object.call(method, url, params, self.limit, endpoint)
        print("   Etsy Update Receipts API Call Complete")
        self.sql_object.update_receipts(data)
        #pp.pprint(data)
        print("Update Etsy Receipts Data Process Complete\n")

    def new_transactions(self):
        print("Retrieving New Etsy Transactions Data")
        url = self.url.get('transactions')
        endpoint = "New Transactions"
        params = {"limit": self.limit,
                  "offset": self.offset,
                  "min_created": self.time}
        method = "Get"
        data = self.api_object.call(method, url, params, self.limit, endpoint)
        print("   Etsy Transactions API Call Complete")
        self.sql_object.new_tranactions(data)
        #pp.pprint(data)
        print("New Etsy Transaction Data Process Complete\n")