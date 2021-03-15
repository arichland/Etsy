_author_ = 'arichland'
import pymysql
import pydict
import pprint
import etsy_api as etsy
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

class sql:
    def listing_id():
        print("   Saving New Receipts to SQL:")
        user = pydict.sql_dict.get('user')
        password = pydict.sql_dict.get('password')
        host = pydict.sql_dict.get('host')
        charset = pydict.sql_dict.get('charset')
        db = pydict.sql_dict.get('db_etsy')
        con = pymysql.connect(user=user, password=password, host=host, database=db, charset=charset)
        skus = []
        with con.cursor() as cur:
            query = """SELECT DISTINCT listing_id from tbl_etsy_listings;"""
            cur.execute(query)
            rows = cur.fetchall()
            for row in rows:
                skus.append(row[0])
        return skus

    def inventory_levels(data):
        pass

class inventory:

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