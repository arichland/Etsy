_author_ = 'arichland'
import pymysql
import pydict
import pprint
import api
import sql
from datetime import datetime, timedelta, timezone, date
pp = pprint.PrettyPrinter(indent=1)

# Setup Epoch timestamp for Etsy API
dt = datetime
td = timedelta
tz = timezone
strip = dt.strptime
strip_format = "%Y-%m-%dT%H:%M:%S"
now = dt.now()
ts = dt.fromtimestamp

class Main:

   def inventory_update():
       skus = sql.listing_id()
       print(skus)
       url = pydict.etsy_urls.get('update_inventory')
       endpoint = "Inventory Update"
       params = {"listing_id": "",
                  "products": "",
                  "price_on_property": "",
                  "quantity_on_property": "",
                  "sku_on_property": ""}

   def listing_products():
       skus = sql.listing_id()
       base = pydict.etsy_urls.get('shop_listings')
       url = "https://openapi.etsy.com/v2/shops/20978352/listings/%s/products?"
       limit = 50
       offset = 0
       filter = ''
       time = ''
       endpoint = "All Active Listings"
       params = {"limit": limit,
                 "offset": offset}
       method = "Get"
       data = etsy.api.call_etsy(method, url, params, limit, endpoint)
       for k in data.values():
           pp.pprint(k)
       # sql.active_listings(data)