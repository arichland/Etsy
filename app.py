_author_ = 'arichland'

import requests
import pprint
from datetime import datetime, timedelta, timezone, date
import pydict
import pymysql
pp = pprint.PrettyPrinter(indent=1)

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

# API URLs
urls = pydict.etsy_urls.get
cusrorType = pymysql.cursors.DictCursor

class sql:

    def new_receipts(data):
        print(" Saving Receipt Data to SQL:")
        user = pydict.sql_dict.get('user')
        password = pydict.sql_dict.get('password')
        host = pydict.sql_dict.get('host')
        charset = pydict.sql_dict.get('charset')
        db = pydict.sql_dict.get('db_etsy')
        con = pymysql.connect(user=user, password=password, host=host, database=db, charset=charset)

        with con.cursor() as cur:
            print("     Creating Temp Table")
            qry_temp_table = """CREATE TEMPORARY TABLE IF NOT EXISTS tbl_temp like etsy.tbl_etsy_receipts;"""
            cur.execute(qry_temp_table)
            con.commit()

            print("     Inserting API Data Into Temp Table")
            for k in data['results']:
                created = ts(k['creation_tsz']).isoformat()
                receipt_id = k["receipt_id"],
                adjusted_grandtotal = float(k['adjusted_grandtotal']),
                buyer_adjusted_grandtotal = float(k['buyer_adjusted_grandtotal']),
                buyer_email = k['buyer_email'],
                buyer_user_id = k['buyer_user_id'],
                city = k['city'],
                currency_code = k['currency_code'],
                days_from_due_date = k['days_from_due_date'],
                day = strip(created, strip_format).day,
                discount_amt = float(k['discount_amt']),
                first_line = k['first_line'],
                gift_message = k['gift_message'],
                grandtotal = float(k['grandtotal']),
                is_gift = k['is_gift'],
                last_modified_est = ts(k['last_modified_tsz']).isoformat(),
                message_from_buyer = k['message_from_buyer'],
                message_from_payment = k['message_from_payment'],
                message_from_seller = k['message_from_seller'],
                month = strip(created, strip_format).month,
                name = k['name'],
                needs_gift_wrap = k['needs_gift_wrap'],
                order_id = k['order_id'],
                payment_email = k['payment_email'],
                payment_method = k['payment_method'],
                quarter = ((strip(created, strip_format).month - 1) // 3) + 1,
                receipt_type = k['receipt_type'],
                second_line = k['second_line'],
                shipped_date = ts(k['shipped_date']).isoformat(),
                state = k['state'],
                subtotal = float(k['subtotal']),
                total_price = float(k['total_price']),
                total_shipping_cost = float(k['total_shipping_cost']),
                total_tax_cost = float(k['total_tax_cost']),
                total_vat_cost = float(k['total_vat_cost']),
                was_paid = k['was_paid'],
                was_shipped = k['was_shipped'],
                week_num = strip(created, strip_format).isocalendar()[1],
                year = strip(created, strip_format).year,
                zip = k['zip']

                qry_temp_data = """Insert into tbl_temp(import_timestamp,
                                                        receipt_id,
                                                        adjusted_grandtotal,
                                                        buyer_adjusted_grandtotal,
                                                        buyer_email,
                                                        buyer_user_id,
                                                        city,
                                                        created,
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
                cur.execute(qry_temp_data, (receipt_id,
                                            adjusted_grandtotal,
                                            buyer_adjusted_grandtotal,
                                            buyer_email,
                                            buyer_user_id,
                                            city,
                                            created,
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

            qry_insert_new_data = """
                                INSERT INTO tbl_etsy_receipts(
                                    import_timestamp,
                                    receipt_id,
                                    adjusted_grandtotal,
                                    buyer_adjusted_grandtotal,
                                    buyer_email,
                                    buyer_user_id,
                                    city,
                                    created,
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
                                    
                                SELECT 
                                    SQ1.import_timestamp,
                                    SQ1.receipt_id,
                                    SQ1.adjusted_grandtotal,
                                    SQ1.buyer_adjusted_grandtotal,
                                    SQ1.buyer_email,
                                    SQ1.buyer_user_id,
                                    SQ1.city,
                                    SQ1.created,
                                    SQ1.currency_code,
                                    SQ1.day,
                                    SQ1.days_from_due_date,
                                    SQ1.discount_amt,
                                    SQ1.first_line,
                                    SQ1.gift_message,
                                    SQ1.grandtotal,
                                    SQ1.is_gift,
                                    SQ1.last_modified_est,
                                    SQ1.message_from_buyer,
                                    SQ1.message_from_payment,
                                    SQ1.message_from_seller,
                                    SQ1.month,
                                    SQ1.name,
                                    SQ1.needs_gift_wrap,
                                    SQ1.order_id,
                                    SQ1.payment_email,
                                    SQ1.payment_method,
                                    SQ1.quarter,
                                    SQ1.receipt_type,
                                    SQ1.second_line,
                                    SQ1.shipped_date,
                                    SQ1.state,
                                    SQ1.subtotal,
                                    SQ1.total_price,
                                    SQ1.total_shipping_cost,
                                    SQ1.total_tax_cost,
                                    SQ1.total_vat_cost,
                                    SQ1.was_paid,
                                    SQ1.was_shipped,
                                    SQ1.week_num,
                                    SQ1.year,
                                    SQ1.zip 

                                    FROM 
                                    (SELECT 
                                    import_timestamp,
                                    receipt_id,
                                    adjusted_grandtotal,
                                    buyer_adjusted_grandtotal,
                                    buyer_email,
                                    buyer_user_id,
                                    city,
                                    created,
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
                                    zip 
                                    FROM tbl_temp) AS SQ1 LEFT JOIN tbl_etsy_receipts ON SQ1.receipt_id = tbl_etsy_receipts.receipt_id WHERE tbl_etsy_receipts.receipt_id IS NULL;"""
            print("     Inserting New Data Into Table")
            cur.execute(qry_insert_new_data)
            con.commit()
        cur.close()
        con.close()
        print(" Process Complete\n")

    def new_trans(data):
        print(" Saving Transaction Data to SQL:")
        user = pydict.sql_dict.get('user')
        password = pydict.sql_dict.get('password')
        host = pydict.sql_dict.get('host')
        charset = pydict.sql_dict.get('charset')
        db = pydict.sql_dict.get('db_etsy')
        con = pymysql.connect(user=user, password=password, host=host, database=db, charset=charset)

        with con.cursor() as cur:
            print("     Creating Temp Table")
            qry_temp_table = """CREATE TEMPORARY TABLE IF NOT EXISTS tbl_temp like etsy.tbl_etsy_trans;"""
            cur.execute(qry_temp_table)
            con.commit()

            print("     Inserting API Data Into Temp Table")
            for k in data['results']:
                transaction_id = k["transaction_id"]
                buyer_user_id = k['buyer_user_id']
                currency_code = k['currency_code']
                created = ts(k['creation_tsz']).isoformat()
                day = strip(created, strip_format).day
                listing_id = k['listing_id']
                month = strip(created, strip_format).month
                price = float((k['price']))
                paid_est = ts(k['paid_tsz']).isoformat()
                quantity = k['quantity']
                quarter = ((strip(created, strip_format).month - 1) // 3) + 1
                receipt_id = k['receipt_id']
                week_num = strip(created, strip_format).isocalendar()[1]
                year = strip(created, strip_format).year

                qry_temp_data = """INSERT INTO tbl_temp(
                import_timestamp,
                buyer_user_id, 
                currency_code, 
                created, 
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
                cur.execute(qry_temp_data, (buyer_user_id,
                                            currency_code,
                                            created,
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

            qry_insert_new_data = """
            INSERT INTO tbl_etsy_trans(
            import_timestamp,
            buyer_user_id, 
            currency_code, 
            created, 
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
            
            SELECT
            SQ1.import_timestamp,
            SQ1.buyer_user_id ,
            SQ1.currency_code ,
            SQ1.created ,
            SQ1.day,
            SQ1.listing_id,
            SQ1.month,
            SQ1.price ,
            SQ1.paid_est ,
            SQ1.quantity,
            SQ1.quarter ,
            SQ1.receipt_id ,
            SQ1.transaction_id,
            SQ1.week_num,
            SQ1.year
            
            FROM (SELECT
            import_timestamp,
            buyer_user_id, 
            currency_code, 
            created, 
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
            year FROM tbl_temp) AS SQ1 LEFT JOIN tbl_etsy_trans ON SQ1.transaction_id = tbl_etsy_trans.transaction_id WHERE tbl_etsy_trans.transaction_id IS NULL;"""
            print("     Inserting New Data Into Table")
            cur.execute(qry_insert_new_data)
            con.commit()
        cur.close()
        con.close()
        print(" Process Complete\n")

    def receipt_updates(data):
        print(" Saving Receipt Data to SQL:")
        user = pydict.sql_dict.get('user')
        password = pydict.sql_dict.get('password')
        host = pydict.sql_dict.get('host')
        charset = pydict.sql_dict.get('charset')
        db = pydict.sql_dict.get('db_etsy')
        con = pymysql.connect(user=user, password=password, host=host, database=db, charset=charset)

        with con.cursor() as cur:
            print("     Creating Temp Table")
            qry_temp_table = """CREATE TEMPORARY TABLE IF NOT EXISTS tbl_temp LIKE etsy.tbl_etsy_receipts;"""
            cur.execute(qry_temp_table)
            con.commit()

            print("     Inserting API Data Into Temp Table")
            for k in data['results']:
                created = ts(k['creation_tsz']).isoformat()
                receipt_id = k["receipt_id"],
                adjusted_grandtotal = float(k['adjusted_grandtotal']),
                buyer_adjusted_grandtotal = float(k['buyer_adjusted_grandtotal']),
                buyer_email = k['buyer_email'],
                buyer_user_id = k['buyer_user_id'],
                city = k['city'],
                currency_code = k['currency_code'],
                days_from_due_date = k['days_from_due_date'],
                day = strip(created, strip_format).day,
                discount_amt = float(k['discount_amt']),
                first_line = k['first_line'],
                gift_message = k['gift_message'],
                grandtotal = float(k['grandtotal']),
                is_gift = k['is_gift'],
                last_modified_est = ts(k['last_modified_tsz']).isoformat(),
                message_from_buyer = k['message_from_buyer'],
                message_from_payment = k['message_from_payment'],
                message_from_seller = k['message_from_seller'],
                month = strip(created, strip_format).month,
                name = k['name'],
                needs_gift_wrap = k['needs_gift_wrap'],
                order_id = k['order_id'],
                payment_email = k['payment_email'],
                payment_method = k['payment_method'],
                quarter = ((strip(created, strip_format).month - 1) // 3) + 1,
                receipt_type = k['receipt_type'],
                second_line = k['second_line'],
                shipped_date = ts(k['shipped_date']).isoformat(),
                state = k['state'],
                subtotal = float(k['subtotal']),
                total_price = float(k['total_price']),
                total_shipping_cost = float(k['total_shipping_cost']),
                total_tax_cost = float(k['total_tax_cost']),
                total_vat_cost = float(k['total_vat_cost']),
                was_paid = k['was_paid'],
                was_shipped = k['was_shipped'],
                week_num = strip(created, strip_format).isocalendar()[1],
                year = strip(created, strip_format).year,
                zip = k['zip']

                qry_insert_temp_data = """Insert into tbl_temp(
                            import_timestamp,
                            receipt_id,
                            adjusted_grandtotal,
                            buyer_adjusted_grandtotal,
                            buyer_email,
                            buyer_user_id,
                            city,
                            created,
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
                cur.execute(qry_insert_temp_data, (receipt_id,
                                                  adjusted_grandtotal,
                                                  buyer_adjusted_grandtotal,
                                                  buyer_email,
                                                  buyer_user_id,
                                                  city,
                                                  created,
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

            print("     Updating Table with API Data")
            qry_update_table = """
            UPDATE tbl_etsy_receipts
            INNER JOIN tbl_temp ON tbl_etsy_receipts.receipt_id = tbl_temp.receipt_id
            SET
            tbl_etsy_receipts.buyer_adjusted_grandtotal = tbl_temp.buyer_adjusted_grandtotal,
            tbl_etsy_receipts.buyer_email = tbl_temp.buyer_email,
            tbl_etsy_receipts.city = tbl_temp.city,
            tbl_etsy_receipts.created = tbl_temp.created,
            tbl_etsy_receipts.currency_code = tbl_temp.currency_code,
            tbl_etsy_receipts.day = tbl_temp.day,
            tbl_etsy_receipts.days_from_due_date = tbl_temp.days_from_due_date,
            tbl_etsy_receipts.discount_amt = tbl_temp.discount_amt,
            tbl_etsy_receipts.first_line = tbl_temp.first_line,
            tbl_etsy_receipts.gift_message = tbl_temp.gift_message,
            tbl_etsy_receipts.grandtotal = tbl_temp.grandtotal,
            tbl_etsy_receipts.is_gift = tbl_temp.is_gift,
            tbl_etsy_receipts.last_modified_est = tbl_temp.last_modified_est,
            tbl_etsy_receipts.message_from_buyer = tbl_temp.message_from_buyer,
            tbl_etsy_receipts.message_from_payment = tbl_temp.message_from_payment,
            tbl_etsy_receipts.message_from_seller = tbl_temp.message_from_seller,
            tbl_etsy_receipts.name = tbl_temp.name,
            tbl_etsy_receipts.needs_gift_wrap = tbl_temp.needs_gift_wrap,
            tbl_etsy_receipts.order_id = tbl_temp.order_id,
            tbl_etsy_receipts.payment_email = tbl_temp.payment_email,
            tbl_etsy_receipts.payment_method = tbl_temp.payment_method,
            tbl_etsy_receipts.receipt_type = tbl_temp.receipt_type,
            tbl_etsy_receipts.second_line = tbl_temp.second_line,
            tbl_etsy_receipts.shipped_date = tbl_temp.shipped_date,
            tbl_etsy_receipts.state = tbl_temp.state,
            tbl_etsy_receipts.subtotal = tbl_temp.subtotal,
            tbl_etsy_receipts.total_price = tbl_temp.total_price,
            tbl_etsy_receipts.total_shipping_cost = tbl_temp.total_shipping_cost,
            tbl_etsy_receipts.total_tax_cost = tbl_temp.total_tax_cost,
            tbl_etsy_receipts.total_vat_cost = tbl_temp.total_vat_cost,
            tbl_etsy_receipts.was_paid = tbl_temp.was_paid,
            tbl_etsy_receipts.was_shipped = tbl_temp.was_shipped,
            tbl_etsy_receipts.zip = tbl_temp.zip;
            """
            cur.execute(qry_update_table)
            con.commit()
        cur.close()
        con.close()
        print(" Process Complete\n")

    def trans_updates(data):
        print(" Saving Transaction Data to SQL:")
        user = pydict.sql_dict.get('user')
        password = pydict.sql_dict.get('password')
        host = pydict.sql_dict.get('host')
        charset = pydict.sql_dict.get('charset')
        db = pydict.sql_dict.get('db_etsy')
        print(" Saving API Data to SQL")
        con = pymysql.connect(user=user, password=password, host=host, database=db, charset=charset)

        with con.cursor() as cur:
            print("     Creating Temp Table")
            qry_temp_table = """CREATE TEMPORARY TABLE IF NOT EXISTS tbl_temp like etsy.tbl_etsy_trans;"""
            cur.execute(qry_temp_table)
            con.commit()

            print("     Inserting API Data Into Temp Table")
            for k in data['results']:
                transaction_id = k["transaction_id"]
                buyer_user_id = k['buyer_user_id']
                currency_code = k['currency_code']
                created = ts(k['creation_tsz']).isoformat()
                day = strip(created, strip_format).day
                listing_id = k['listing_id']
                month = strip(created, strip_format).month
                price = float((k['price']))
                paid_est = ts(k['paid_tsz']).isoformat()
                quantity = k['quantity']
                quarter = ((strip(created, strip_format).month - 1) // 3) + 1
                receipt_id = k['receipt_id']
                week_num = strip(created, strip_format).isocalendar()[1]
                year = strip(created, strip_format).year

                qry_temp_data = """INSERT INTO tbl_temp(
                import_timestamp,
                buyer_user_id, 
                currency_code, 
                created, 
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
                cur.execute(qry_temp_data, (buyer_user_id,
                                            currency_code,
                                            created,
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

            print("     Updating Table with API Data")
            qry_update_table = """
            UPDATE tbl_etsy_trans
            INNER JOIN tbl_temp ON tbl_etsy_trans.transaction_id = tbl_temp.transaction_id
            SET
            tbl_etsy_trans.import_timestamp = tbl_temp.import_timestamp,
            tbl_etsy_trans.buyer_user_id = tbl_temp.buyer_user_id,
            tbl_etsy_trans.currency_code = tbl_temp.currency_code,
            tbl_etsy_trans.created  = tbl_temp.created,
            tbl_etsy_trans.day = tbl_temp.day,
            tbl_etsy_trans.listing_id = tbl_temp.listing_id,
            tbl_etsy_trans.month = tbl_temp.month,
            tbl_etsy_trans.price = tbl_temp.price,
            tbl_etsy_trans.paid_est = tbl_temp.paid_est,
            tbl_etsy_trans.quantity = tbl_temp.quantity,
            tbl_etsy_trans.quarter = tbl_temp.quarter,
            tbl_etsy_trans.receipt_id = tbl_temp.receipt_id,
            tbl_etsy_trans.transaction_id = tbl_temp.transaction_id,
            tbl_etsy_trans.week_num = tbl_temp.week_num,
            tbl_etsy_trans.year = tbl_temp.year;"""
            cur.execute(qry_update_table)
            con.commit()
        cur.close()
        con.close()
        print(" Process Complete\n")

class api:
    # API Auth Fields
    api = pydict.new_app_api_auth.get
    token = api('oauth_token')
    key = api('oauth_consumer_key')
    nonce = api('oauth_nonce')
    sign = api('oauth_signature')
    header = pydict.api_header

    def update_time():
        utc_sec = td(seconds=18000)
        yd_sec = td(seconds=86400)
        yd_utc = (now + utc_sec)-yd_sec
        #update_time = dt(year=yd_utc.year, month=yd_utc.month, day=yd_utc.day, hour=23, minute=59, second=59)
        update_time = "2019-12-31 23:59:59"
        return update_time

    def api_log(time, market, ep, code, descr):
        user = pydict.sql_dict.get('user')
        password = pydict.sql_dict.get('password')
        host = pydict.sql_dict.get('host')
        charset = pydict.sql_dict.get('charset')
        db = "rtcaws01"  # sql('db_rtc')
        con = pymysql.connect(user=user, password=password, host=host, database=db, charset=charset)

        with con.cursor() as cur:
            qry_update = """INSERT INTO tbl_api_log(timestamp, market, endpoint, api_code, code_descr) VALUES(%s, %s, %s, %s, %s);"""
            #endpoint = log["endpoint"]
            #api_code = log["api_code"]
            #code_descr = log["code_descr"]
            cur.execute(qry_update, (time, market, ep, code, descr))
            con.commit()
        cur.close()
        con.close()

    def api_call(str, param, time, type):
        print("Starting API Call")
        endpoint = pydict.etsy_urls.get(str)
        api_log = {}
        limit = 50
        offset = 0
        calls = 0
        params = {"limit": limit,
                  "offset": offset,
                  param: time}
        header = pydict.api_header
        payload = {}
        response = requests.get(endpoint, params=params, headers=header, json=payload)
        data = response.json()
        records_received = len([len(data) for i in data['results']])

        while records_received == limit:
            calls += 1
            offset = calls * limit
            params = {"limit": limit,
                  "offset": offset,
                  param: time}
            response = requests.get(endpoint, params=params, headers=header, json=payload)
            data = response.json()
            api = "etsy"
            code = response.status_code
            descr = pydict.api_codes.get(api).get(code)
            records_received = len([len(data) for i in data['results']])
            print("API Call:", calls, "| Records:", records_received, "| Offset:", offset)
            test = pydict.api_codes.get("etsy").get(200)

            if str == "receipts":
                if type == "New":
                    timestamp = time,
                    market = 'Etsy',
                    ep = 'New Receipts',
                    api_code = code,
                    code_descr = "descr"
                    #api_log(timestamp, market, ep, code, descr)
                    sql.new_receipts(data)
                else:
                    log = {"timestamp": time,
                           "endpoint": 'Update Receipts',
                           "api": "Etsy",
                           'market': 'Etsy',
                           'response_code': code,
                           'response_descr': descr}
                    sql.receipt_updates(data)
            elif str == "trans":
                if type == "New":
                    log = {"timestamp": time,
                            "market": 'Etsy',
                            "endpoint": 'New Transactions',
                            'api_code': code,
                            'code_descr': descr}
                    #api.api_log(log)
                    sql.new_trans(data)
                else:
                    log = {"timestamp": time,
                           "endpoint": 'Update Transactions',
                           "api": "Etsy",
                           'market': 'Etsy',
                           'response_code': code,
                           'response_descr': descr}
                    sql.trans_updates(data)
            else:
                pass


def new_receipts():
    time = api.update_time()
    param = "min_created"
    type = "New"
    api.api_call("receipts", param, time, type)

def update_receipts():
    time = api.update_time()
    param = "min_last_modified"
    type = "Update"
    api.api_call("receipts", param, time, type)

def new_trans():
    time = api.update_time()
    param = "min_created"
    type = "New"
    api.api_call("trans", param, time, type)

def update_trans():
    time = api.update_time()
    param = "min_last_modified"
    type = "Update"
    api.api_call("trans", param, time, type)

def functions():
    new_receipts()
    #new_trans()
    #update_receipts()
    #update_trans()
functions()