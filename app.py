_author_ = 'arichland'

import json
import requests
import pprint
from datetime import datetime, timedelta, timezone, date
import pydict
import pymysql

# Setup formatting
pp = pprint.PrettyPrinter(indent=1)


# Setup Epoch timestamp for Etsy API
dt = datetime
td = timedelta
tz = timezone
strip = dt.strptime
strip_format = "%Y-%m-%dT%H:%M:%S"
#date = dt.date
now = dt.now()
utc_sec = td(seconds=18000)
sec = td(seconds=86400)
ts = dt.fromtimestamp

# API Auth Fields
api = pydict.new_app_api_auth.get
token = api('oauth_token')
key = api('oauth_consumer_key')
nonce = api('oauth_nonce')
sign = api('oauth_signature')
header = pydict.api_header


# API URLs
url = pydict.etsy_data_url.get
new_receipts = url('new_receipts')
hist_receipts = url('hist_recepts')


# SQL DB Connection Fields
sql = pydict.sql_dict.get
user = sql('user')
password = sql('password')
host = sql('host')
charset = sql('charset')
cusrorType = pymysql.cursors.DictCursor

# Data dictionaries
payload = {}
trans = {}
receipts = {}
api_log = {}

# SQL Functions
def update_time():
    utc_sec = td(seconds=18000)
    yd_sec = td(seconds=86400)
    yd_utc = (now + utc_sec)-yd_sec
    update_time = dt(year=yd_utc.year, month=yd_utc.month, day=yd_utc.day, hour=23, minute=59, second=59)
    print(update_time)
    return update_time

def api_call_log():
    db = sql('db_rtc')
    print("   API Call Log: Start")
    con = pymysql.connect(user=user,
                          password=password,
                          host=host,
                          database=db,
                          charset=charset,
                          cursorclass=cusrorType)

    for val in api_log.values():
        data = val['update_type']
        api = val['market']
        api_code = val['response_code']
        code_descr = val['response_descr']

        qry_update = """INSERT INTO tbl_API_Log(timestamp, data, api, api_code, code_descr) VALUES(Now(), %s, %s, %s, %s);"""
        with con.cursor() as cur:
            cur.execute(qry_update, (data,
                                     api,
                                     api_code,
                                     code_descr))
        con.commit()

    print("   API Call Log: Complete")

def sql_etsy_trans():
    db = sql('db_etsy')
    print("   Etsy - Transaction Data to SQL DB:", "Start")
    con = pymysql.connect(user=user,
                          password=password,
                          host=host,
                          database=db,
                          charset=charset,
                          cursorclass=cusrorType)

    for val in trans.values():
        buyer_user_id = val['buyer_user_id']
        currency_code = val['currency_code']
        creation_est = val['creation_est']
        day = val['day']
        listing_id = val['listing_id']
        month = val['month']
        price = val['price']
        paid_est = val['paid_est']
        quantity = val['quantity']
        quarter = val['quarter']
        receipt_id = val['receipt_id']
        transaction_id = val['transaction_id']
        week_num = val['week_num']
        year = val['year']

        qry_insert_trans = """INSERT INTO tbl_Etsy_Trans(
        import_timestamp,
        buyer_user_id, 
        currency_code, 
        creation_est, 
        day,
        listing_id,
        month,
        price, 
        paid_est, 
        quantity,
        quarter, 
        receipt_id, 
        transaction_id,
        week_num,
        year) 
        Values(Now(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

        with con.cursor() as cur:
            cur.execute(qry_insert_trans,
                        (buyer_user_id,
                         currency_code,
                         creation_est,
                         day,
                         listing_id,
                         month,
                         price,
                         paid_est,
                         quantity,
                         quarter,
                         receipt_id,
                         transaction_id,
                         week_num,
                         year))
        con.commit()
    print("   Etsy - Transaction Data to SQL DB: Complete")

def sql_etsy_receipts():
    db = sql('db_etsy')
    print("   Etsy - Receipt Data to SQL DB: Start")
    con = pymysql.connect(user=user,
                          password=password,
                          host=host,
                          database=db,
                          charset=charset,
                          cursorclass=cusrorType)

    for val in receipts.values():
        receipt_id = val['receipt_id']
        adjusted_grandtotal = val['adjusted_grandtotal']
        buyer_adjusted_grandtotal = val['buyer_adjusted_grandtotal']
        buyer_email = val['buyer_email']
        buyer_user_id = val['buyer_user_id']
        city = val['city']
        creation_est = val['creation_est']
        currency_code = val['currency_code']
        day = val['day']
        days_from_due_date = val['days_from_due_date']
        discount_amt = val['discount_amt']
        first_line = val['first_line']
        gift_message = val['gift_message']
        grandtotal = val['grandtotal']
        is_gift = val['is_gift']
        last_modified_est = val['last_modified_est']
        message_from_buyer = val['message_from_buyer']
        message_from_payment = val['message_from_payment']
        message_from_seller = val['message_from_seller']
        month = val['month']
        name = val['name']
        needs_gift_wrap = val['needs_gift_wrap']
        order_id = val['order_id']
        payment_email = val['payment_email']
        payment_method = val['payment_method']
        quarter = val['quarter']
        receipt_type = val['receipt_type']
        second_line = val['second_line']
        shipped_date = val['shipped_date']
        state = val['state']
        subtotal = val['subtotal']
        total_price = val['total_price']
        total_shipping_cost = val['total_shipping_cost']
        total_tax_cost = val['total_tax_cost']
        total_vat_cost = val['total_vat_cost']
        was_paid = val['was_paid']
        was_shipped = val['was_shipped']
        week_num = val['week_num']
        year = val['year']
        zip = val['zip']

        qry_insert_receipts = """Insert into tbl_Etsy_Receipts(
        import_timestamp,
        receipt_id,
        adjusted_grandtotal,
        buyer_adjusted_grandtotal,
        buyer_email,
        buyer_user_id,
        city,
        creation_est,
        currency_code,
        day,
        days_from_due_date,
        discount_amt,
        first_line,
        gift_message,
        grandtotal,
        is_gift,
        last_modified_est,
        message_from_buyer,
        message_from_payment,
        message_from_seller,
        month,
        name,
        needs_gift_wrap,
        order_id,
        payment_email,
        payment_method,
        quarter,
        receipt_type,
        second_line,
        shipped_date,
        state,
        subtotal,
        total_price,
        total_shipping_cost,
        total_tax_cost,
        total_vat_cost,
        was_paid,
        was_shipped,
        week_num,
        year,
        zip) 
        Values(Now(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        with con.cursor() as cur:
            cur.execute(qry_insert_receipts, (receipt_id,
                                              adjusted_grandtotal,
                                              buyer_adjusted_grandtotal,
                                              buyer_email,
                                              buyer_user_id,
                                              city,
                                              creation_est,
                                              currency_code,
                                              day,
                                              days_from_due_date,
                                              discount_amt,
                                              first_line,
                                              gift_message,
                                              grandtotal,
                                              is_gift,
                                              last_modified_est,
                                              message_from_buyer,
                                              message_from_payment,
                                              message_from_seller,
                                              month,
                                              name,
                                              needs_gift_wrap,
                                              order_id,
                                              payment_email,
                                              payment_method,
                                              quarter,
                                              receipt_type,
                                              second_line,
                                              shipped_date,
                                              state,
                                              subtotal,
                                              total_price,
                                              total_shipping_cost,
                                              total_tax_cost,
                                              total_vat_cost,
                                              was_paid,
                                              was_shipped,
                                              week_num,
                                              year,
                                              zip))
        con.commit()
    print("   Etsy - Receipt Data to SQL DB: Complete")



# API Functions
def api_etsy_hist_trans():
    print("\nEsty - Historical Transactions API Call: Start")
    count = 0
    api_url = ["https://openapi.etsy.com/v2/shops/20978352/transactions?limit=100&offset=0",
               "https://openapi.etsy.com/v2/shops/20978352/transactions?limit=100&offset=100",
               "https://openapi.etsy.com/v2/shops/20978352/transactions?limit=100&offset=200",
               "https://openapi.etsy.com/v2/shops/20978352/transactions?limit=100&offset=300"
               ]
    for i in api_url:
        count += 1
        response = requests.get(i, headers=header, json=payload)
        r = json.loads(response.text)
        results = r['results']

        for k in results:
            count += 1
            product_data = k['product_data']
            variations = k['variations']
            created = ts(k['creation_tsz']).isoformat()
            temp = {count: {'transaction_id': k["transaction_id"],
                    'buyer_user_id': k['buyer_user_id'],
                    'currency_code': k['currency_code'],
                    'creation_est': created,
                    'day': strip(created, strip_format).day,
                    'listing_id': k['listing_id'],
                    'month': strip(created, strip_format).month,
                    'price': float((k['price'])),
                    'paid_est': ts(k['paid_tsz']).isoformat(),
                    'quantity': k['quantity'],
                    'quarter': ((strip(created, strip_format).month - 1) // 3) + 1,
                    'receipt_id': k['receipt_id'],
                    'week_num': strip(created, strip_format).isocalendar()[1],
                    'year': strip(created, strip_format).year
                    }}
            trans.update(temp)
    #sql_etsy_trans()
    print("Esty - Historical Transactions API Call: Complete")

def api_etsy_hist_receipts():
    print("\nEtsy - Historical Receipts API Call: Start")
    receipts_urls = ["https://openapi.etsy.com/v2/shops/20978352/receipts?&limit=100&offset=0",
                     "https://openapi.etsy.com/v2/shops/20978352/receipts?&limit=100&offset=100",
                     "https://openapi.etsy.com/v2/shops/20978352/receipts?&limit=100&offset=200",
                     "https://openapi.etsy.com/v2/shops/20978352/receipts?&limit=100&offset=300",
                     "https://openapi.etsy.com/v2/shops/20978352/receipts?&limit=100&offset=400"]
    count = 0

    for i in receipts_urls:
        count += 1
        response = requests.get(i, headers=header, json=payload)
        r = json.loads(response.text)
        results = r['results']

        for k in results:
            created = ts(k['creation_tsz']).isoformat()
            count += 1
            dict = {count: {'receipt_id': k["receipt_id"],
                            'adjusted_grandtotal': float(k['adjusted_grandtotal']),
                            'buyer_adjusted_grandtotal': float(k['buyer_adjusted_grandtotal']),
                            'buyer_email': k['buyer_email'],
                            'buyer_user_id': k['buyer_user_id'],
                            'city': k['city'],
                            'creation_est': created,
                            'currency_code': k['currency_code'],
                            'days_from_due_date': k['days_from_due_date'],
                            'day': strip(created, strip_format).day,
                            'discount_amt': float(k['discount_amt']),
                            'first_line': k['first_line'],
                            'gift_message': k['gift_message'],
                            'grandtotal': float(k['grandtotal']),
                            'is_gift': k['is_gift'],
                            'last_modified_est': ts(k['last_modified_tsz']).isoformat(),
                            'message_from_buyer': k['message_from_buyer'],
                            'message_from_payment': k['message_from_payment'],
                            'message_from_seller': k['message_from_seller'],
                            'month': strip(created, strip_format).month,
                            'name': k['name'],
                            'needs_gift_wrap': k['needs_gift_wrap'],
                            'order_id': k['order_id'],
                            'payment_email': k['payment_email'],
                            'payment_method': k['payment_method'],
                            'quarter': ((strip(created, strip_format).month-1)//3)+1,
                            'receipt_type': k['receipt_type'],
                            'second_line': k['second_line'],
                            'shipped_date': ts(k['shipped_date']).isoformat(),
                            'state': k['state'],
                            'subtotal': float(k['subtotal']),
                            'total_price': float(k['total_price']),
                            'total_shipping_cost': float(k['total_shipping_cost']),
                            'total_tax_cost': float(k['total_tax_cost']),
                            'total_vat_cost': float(k['total_vat_cost']),
                            'was_paid': k['was_paid'],
                            'was_shipped': k['was_shipped'],
                            'week_num': strip(created, strip_format).isocalendar()[1],
                            'year': strip(created, strip_format).year,
                            'zip': k['zip']}}
            receipts.update(dict)
    #sql_etsy_receipts()
    pp.pprint(receipts)
    print("Etsy - Historical Receipts API Call: Complete")

def api_etsy_new_receipts():
    print("\nEtsy - New Receipt Data API Call: Start")
    count = 0
    api_url = "https://openapi.etsy.com/v2/shops/20978352/receipts?findAllShopReceipts&limit=100&min_last_modified=%s" %(update_time())
    response = requests.get(api_url, headers=header, json=payload)
    api_code = response.status_code
    r = json.loads(response.text)

    # Lookup API code in api_code_dict and insert into api_log dict
    etsy = pydict.api_code_dict.get('Etsy')
    code_descr = etsy[api_code]
    temp = {'Esty_Receipts': {
        'timestamp': update_time(),
        'update_type': 'Receipts',
        'market': 'Etsy',
        'response_code': api_code,
        'response_descr': code_descr}}
    api_log.update(temp)

    if api_code != 200:
        api_call_log()
        api_log.clear()

    # Insert API data into receipts_dict for use in SQL
    else:
        api_call_log()
        api_log.clear()
        results = r['results']
        for k in results:
            count += 1
            temp = {count: {'receipt_id': k["receipt_id"],
                        'adjusted_grandtotal': float(k['adjusted_grandtotal']),
                        'buyer_adjusted_grandtotal': float(k['buyer_adjusted_grandtotal']),
                        'buyer_email': k['buyer_email'],
                        'buyer_user_id': k['buyer_user_id'],
                        'city': k['city'],
                        'creation_est': ts(k['creation_tsz']).isoformat(),
                        'currency_code': k['currency_code'],
                        'days_from_due_date': k['days_from_due_date'],
                        'discount_amt': float(k['discount_amt']),
                        'first_line': k['first_line'],
                        'gift_message': k['gift_message'],
                        'grandtotal': float(k['grandtotal']),
                        'is_gift': k['is_gift'],
                        'last_modified_est': ts(k['last_modified_tsz']).isoformat(),
                        'message_from_buyer': k['message_from_buyer'],
                        'message_from_payment': k['message_from_payment'],
                        'message_from_seller': k['message_from_seller'],
                        'name': k['name'],
                        'needs_gift_wrap': k['needs_gift_wrap'],
                        'order_id': k['order_id'],
                        'payment_email': k['payment_email'],
                        'payment_method': k['payment_method'],
                        'receipt_type': k['receipt_type'],
                        'second_line': k['second_line'],
                        'shipped_date': ts(k['shipped_date']).isoformat(),
                        'state': k['state'],
                        'subtotal': float(k['subtotal']),
                        'total_price': float(k['total_price']),
                        'total_shipping_cost': float(k['total_shipping_cost']),
                        'total_tax_cost': float(k['total_tax_cost']),
                        'total_vat_cost': float(k['total_vat_cost']),
                        'was_paid': k['was_paid'],
                        'was_shipped': k['was_shipped'],
                        'zip': k['zip']}}
            receipts.update(temp)
        pp.pprint(receipts)
        #sql_etsy_receipts()
    print("Etsy - New Receipt Data API Call: Complete")

def api_etsy_new_trans():
    print("\nEtsy - New Transaction Data API Call: Start")
    count = 0
    api_url = "https://openapi.etsy.com/v2/shops/20978352/transactions?findAllListingTransactions&limit=100&min_last_modified=%s" %(update_time())
    response = requests.get(api_url, headers=header, json=payload)
    api_code = response.status_code
    r = json.loads(response.text)

    # Lookup API code in api_code_dict and insert into api_log dict
    etsy = pydict.api_code_dict.get('Etsy')
    code_descr = etsy[api_code]
    temp = {'Esty_Transactions': {
        'timestamp': update_time(),
        'update_type': 'Transactions',
        'market': 'Etsy',
        'response_code': api_code,
        'response_descr': code_descr}}
    api_log.update(temp)

    if api_code != 200:
        api_call_log()
        api_log.clear()

    else:
        api_call_log()
        api_log.clear()
        results = r['results']
        for k in results:
            count += 1
            created = ts(k['creation_tsz']).isoformat()
            temp = {count: {'transaction_id': k["transaction_id"],
                            'buyer_user_id': k['buyer_user_id'],
                            'currency_code': k['currency_code'],
                            'creation_est': created,
                            'day': strip(created, strip_format).day,
                            'listing_id': k['listing_id'],
                            'month': strip(created, strip_format).month,
                            'price': float((k['price'])),
                            'paid_est': ts(k['paid_tsz']).isoformat(),
                            'quantity': k['quantity'],
                            'quarter': ((strip(created, strip_format).month - 1) // 3) + 1,
                            'receipt_id': k['receipt_id'],
                            'week_num': strip(created, strip_format).isocalendar()[1],
                            'year': strip(created, strip_format).year
                            }}
            trans.update(temp)
        #sql_etsy_trans()
    print("Etsy - New Transaction Data API Call: Comlpete")



def etsy_hist_data():
    api_etsy_hist_trans()
    api_etsy_hist_receipts()
#etsy_hist_data()


def etsy_new_data():
    api_etsy_new_receipts()
    api_etsy_new_trans()
etsy_new_data()
