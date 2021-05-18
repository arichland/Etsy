_author_ = 'arichland'

import pydict
import pprint
import api
import sql
pp = pprint.PrettyPrinter(indent=1)

class Main:
    def __init__(self):
        self.api_object = api.Main()
        self.sql_object = sql.OrderQuery()
        self.url = pydict.etsy_urls
        self.time = self.api_object.update_time()
        self.limit = 50
        self.offset = 0

    def update_listing(self):
        base = self.url.get('base')
        url = base+"/listing/"
        filter = ''
        time = ''
        endpoint = "Update Listings"
        params = {"limit": self.limit,
                  "offset": self.offset}
        method = "Get"
        data = self.api_object.call(method, url, params, self.limit, endpoint)
        pp.pprint(data)
        #for k in data.values():
        #    pp.pprint(k)
        #sql.listings_active(data)

    def listings(self):
        url = self.url.get('shop_listings')
        limit = 50
        offset = 0
        filter = ''
        time = ''
        endpoint = "All Active Listings"
        params = {"limit": self.limit,
                  "offset": self.offset}
        method = "Get"
        data = self.api_object.call(method, url, params, self.limit, endpoint)
        for k in data.values():
            tags = k['tags']
            ",".join(tags)
            print(",".join(tags))
        #sql.listings_active_to_sql(data)
        pp.pprint(data)

    def products(self):
        url = self.url.get('listing_product')
        filter = ''
        time = ''
        endpoint = "All Active Listings"
        params = {"limit": self.limit,
                  "offset": self.offset}
        method = "Get"
        data = self.api_object.call(method, url, params, self.limit, endpoint)
        pp.pprint(data)

    def metrics(self):
        print("\nRetrieving Etsy Listing Metrics")
        url = self.url.get('shop_listings')
        filter = ''
        time = self.time
        endpoint = "Listings Metrics"
        params = {"limit": self.limit,
                  "offset": self.offset}
        method = "Get"
        data = self.api_object.call(method, url, params, self.limit, endpoint)
        pp.pprint(data)
        #sql.listings_metrics_to_sql(data)