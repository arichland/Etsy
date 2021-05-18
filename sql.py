_author_ = 'arichland'

import pymysql
import pydict
from datetime import datetime, timedelta, timezone, date

class CreateTable:
    # Creates tables for SQL database

    def __init__(self):
        self.dict = pydict.sql_dict.get
        self.user = self.dict('user')
        self.password = self.dict('password')
        self.host = self.dict('host')
        self.db = self.dict('db_etsy')

    def receipts(self):
        con = pymysql.connect(user=self.user, password=self.password, host=self.host, database=self.db)

        with con.cursor() as cur:
            qry_tbl_receipts = """CREATE TABLE IF NOT EXISTS tbl_etsy_receipts(
                id INT AUTO_INCREMENT PRIMARY KEY,
                receipt_id BIGINT,adjusted_grandtotal DOUBLE,
                buyer_adjusted_grandtotal DOUBLE,
                buyer_email TEXT,
                buyer_user_id BIGINT,
                city TEXT,
                created DATETIME,
                currency_code TEXT,
                days_from_due_date INT,
                day INT,
                discount_amt DOUBLE,
                first_line TEXT,
                gift_message TEXT,
                grandtotal DOUBLE,
                is_gift TEXT,
                import_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_modified_est DATETIME,
                message_from_buyer TEXT,
                message_from_payment TEXT,
                message_from_seller TEXT,
                month INT,
                name TEXT,
                needs_gift_wrap TEXT,
                order_id BIGINT,
                payment_email TEXT,
                payment_method TEXT,
                quarter INT,
                receipt_type TEXT,
                second_line TEXT,
                shipped_date DATETIME,
                state TEXT,
                status TEXT,
                subtotal DOUBLE,
                total_price DOUBLE,
                total_shipping_cost DOUBLE,
                total_tax_cost DOUBLE,
                total_vat_cost DOUBLE,
                was_paid TEXT,
                was_shipped TEXT,
                week_num INT,
                year INT,
                zip TEXT)
                ENGINE=INNODB;"""
            cur.execute(qry_tbl_receipts)
            con.commit()
        cur.close()
        con.close()

    def transactions(self):
        con = pymysql.connect(user=self.user, password=self.password, host=self.host, database=self.db)

        with con.cursor() as cur:
            qry_tbl_transactions = """CREATE TABLE IF NOT EXISTS tbl_etsy_trans(
                id INT AUTO_INCREMENT PRIMARY KEY,
                buyer_user_id BIGINT,
                currency_code TEXT,
                created DATETIME,
                day INT,
                import_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                listing_id BIGINT,
                month INT,
                price DOUBLE,
                paid_est DATETIME,
                product_id BIGINT,
                quantity INT,
                quarter INT,
                receipt_id BIGINT,
                sku TEXT,
                transaction_id BIGINT,
                week_num INT,
                year INT)
                ENGINE=INNODB;"""
            cur.execute(qry_tbl_transactions)
            con.commit()
        cur.close()
        con.close()

    def api_log(self):
        con = pymysql.connect(user=self.user, password=self.password, host=self.host, database=self.db)

        with con.cursor() as cur:
            qry_create_table = """CREATE TABLE IF NOT EXISTS tbl_api_log(
            id INT AUTO_INCREMENT PRIMARY KEY,
            timestamp DATETIME,
            epoch DATETIME,
            market TEXT,
            endpoint TEXT,
            api_code INT,
            code_descr TEXT)
            ENGINE=INNODB;"""
            cur.execute(qry_create_table)
        cur.close()
        con.close()

    def listings(self):
        con = pymysql.connect(user=self.user, password=self.password, host=self.host, database=self.db)

        with con.cursor() as cur:
            qry_create_table = """CREATE TABLE IF NOT EXISTS tbl_etsy_listings(
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    import_timestamp DATETIME,
                    created_date DATETIME,
                    currency_code TEXT,
                    ending_date DATETIME,
                    description TEXT,
                    featured_rank INT,
                    has_variations BOOLEAN,
                    is_customizable BOOLEAN,
                    item_dimensions_unit TEXT,
                    item_height INT,
                    item_length INT,
                    item_weight INT,
                    item_weight_unit TEXT,
                    item_width INT,
                    last_modified_date DATETIME,
                    listing_id BIGINT,
                    materials TEXT,
                    non_taxable BOOLEAN,
                    num_favorers INT,
                    occasion TEXT,
                    original_created_date DATETIME,
                    price DOUBLE,
                    processing_max INT,
                    processing_min INT,
                    quantity INT,
                    recipient TEXT,
                    shipping_profile_id BIGINT,
                    shipping_template_id BIGINT,
                    shop_section_id BIGINT,
                    should_auto_renew BOOLEAN,
                    state TEXT,
                    state_tsz DATETIME,
                    style TEXT,
                    tags TEXT,
                    taxonomy_id INT,
                    title TEXT,
                    url TEXT,
                    user_id BIGINT,
                    views INT,
                    when_made TEXT,
                    who_made TEXT)
                    ENGINE=INNODB;"""
            cur.execute(qry_create_table)
            cur.close()
            con.close()

    def metrics(self):
        con = pymysql.connect(user=self.user, password=self.password, host=self.host, database=self.db)

        with con.cursor() as cur:
            qry_create_table = """CREATE TABLE IF NOT EXISTS tbl_etsy_metrics(
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    timestamp DATETIME,                
                    featured_rank INT,                
                    listing_id BIGINT,                
                    num_favorers INT,                
                    title TEXT,                
                    views INT)
                    ENGINE=INNODB;"""
            cur.execute(qry_create_table)
            cur.close()
            con.close()

class OrderQuery:
    # All queries for inputting API into SQL database

    def __init__(self):
        self.dict = pydict.sql_dict.get
        self.user = self.dict('user')
        self.password = self.dict('password')
        self.host = self.dict('host')
        self.db = self.dict('db_etsy')
        self.dt = datetime
        self.td = timedelta
        self.tz = timezone
        self.strip = self.dt.strptime
        self.strip_format = "%Y-%m-%dT%H:%M:%S"
        self.now = self.dt.now()
        self.ts = self.dt.fromtimestamp

    def new_receipts(self, data):
        print("   Saving New Receipts to SQL:")
        con = pymysql.connect(user=self.user, password=self.password, host=self.host, database=self.db)

        with con.cursor() as cur:
            print("   Creating Temp Table")
            qry_temp_table = """CREATE TEMPORARY TABLE IF NOT EXISTS tbl_temp like etsy.tbl_etsy_receipts;"""
            cur.execute(qry_temp_table)
            con.commit()

            print("   Inserting Data Into Temp Table")
            for k in data.values():
                created = self.ts(k['creation_tsz']).isoformat()
                receipt_id = k["receipt_id"],
                adjusted_grandtotal = float(k['adjusted_grandtotal']),
                buyer_adjusted_grandtotal = float(k['buyer_adjusted_grandtotal']),
                buyer_email = k['buyer_email'],
                buyer_user_id = k['buyer_user_id'],
                city = k['city'],
                currency_code = k['currency_code'],
                days_from_due_date = k['days_from_due_date'],
                day = self.strip(created, self.strip_format).day,
                discount_amt = float(k['discount_amt']),
                first_line = k['first_line'],
                gift_message = k['gift_message'],
                grandtotal = float(k['grandtotal']),
                is_gift = k['is_gift'],
                last_modified_est = self.ts(k['last_modified_tsz']).isoformat(),
                message_from_buyer = k['message_from_buyer'],
                message_from_payment = k['message_from_payment'],
                message_from_seller = k['message_from_seller'],
                month = self.strip(created, self.strip_format).month,
                name = k['name'],
                needs_gift_wrap = k['needs_gift_wrap'],
                order_id = k['order_id'],
                payment_email = k['payment_email'],
                payment_method = k['payment_method'],
                quarter = ((self.strip(created, self.strip_format).month - 1) // 3) + 1,
                receipt_type = k['receipt_type'],
                second_line = k['second_line'],
                shipped_date = self.ts(k['shipped_date']).isoformat(),
                state = k['state'],
                subtotal = float(k['subtotal']),
                total_price = float(k['total_price']),
                total_shipping_cost = float(k['total_shipping_cost']),
                total_tax_cost = float(k['total_tax_cost']),
                total_vat_cost = float(k['total_vat_cost']),
                was_paid = k['was_paid'],
                was_shipped = k['was_shipped'],
                week_num = self.strip(created, self.strip_format).isocalendar()[1],
                year = self.strip(created, self.strip_format).year,
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
                                    status,
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
                                    SQ1.status,
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
                                    "Open" AS status,
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
            print("   Inserting New Data Into Table")
            cur.execute(qry_insert_new_data)
            con.commit()
        cur.close()
        con.close()
        print("   New Receipts Entered into SQL")

    def update_receipts(self, data):
        print("   Saving Receipt Updates to SQL:")
        con = pymysql.connect(user=self.user, password=self.password, host=self.host, database=self.db)

        with con.cursor() as cur:
            print("   Creating Temp Table")
            qry_temp_table = """CREATE TEMPORARY TABLE IF NOT EXISTS tbl_temp LIKE etsy.tbl_etsy_receipts;"""
            cur.execute(qry_temp_table)
            con.commit()

            print("   Inserting API Data Into Temp Table")
            for k in data.values():
                created = self.ts(k['creation_tsz']).isoformat()
                receipt_id = k["receipt_id"],
                adjusted_grandtotal = float(k['adjusted_grandtotal']),
                buyer_adjusted_grandtotal = float(k['buyer_adjusted_grandtotal']),
                buyer_email = k['buyer_email'],
                buyer_user_id = k['buyer_user_id'],
                city = k['city'],
                currency_code = k['currency_code'],
                days_from_due_date = k['days_from_due_date'],
                day = self.strip(created, self.strip_format).day,
                discount_amt = float(k['discount_amt']),
                first_line = k['first_line'],
                gift_message = k['gift_message'],
                grandtotal = float(k['grandtotal']),
                is_gift = k['is_gift'],
                last_modified_est = self.ts(k['last_modified_tsz']).isoformat(),
                message_from_buyer = k['message_from_buyer'],
                message_from_payment = k['message_from_payment'],
                message_from_seller = k['message_from_seller'],
                month = self.strip(created, self.strip_format).month,
                name = k['name'],
                needs_gift_wrap = k['needs_gift_wrap'],
                order_id = k['order_id'],
                payment_email = k['payment_email'],
                payment_method = k['payment_method'],
                quarter = ((self.strip(created, self.strip_format).month - 1) // 3) + 1,
                receipt_type = k['receipt_type'],
                second_line = k['second_line'],
                shipped_date = self.ts(k['shipped_date']).isoformat(),
                state = k['state'],
                subtotal = float(k['subtotal']),
                total_price = float(k['total_price']),
                total_shipping_cost = float(k['total_shipping_cost']),
                total_tax_cost = float(k['total_tax_cost']),
                total_vat_cost = float(k['total_vat_cost']),
                was_paid = k['was_paid'],
                was_shipped = k['was_shipped'],
                week_num = self.strip(created, self.strip_format).isocalendar()[1],
                year = self.strip(created, self.strip_format).year,
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

            print("   Updating Table with API Data")
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
        print("   Receipts Updated in SQL")

    def new_tranactions(self, data):
        print("   Saving Transaction Data to SQL:")
        con = pymysql.connect(user=self.user, password=self.password, host=self.host, database=self.db)

        with con.cursor() as cur:
            print("   Creating Temp Table")
            qry_temp_table = """CREATE TEMPORARY TABLE IF NOT EXISTS tbl_temp like etsy.tbl_etsy_trans;"""
            cur.execute(qry_temp_table)
            con.commit()

            print("   Inserting Data Into Temp Table")
            for k in data.values():
                transaction_id = k['transaction_id']
                buyer_user_id = k['buyer_user_id']
                currency_code = k['currency_code']
                created = self.ts(k['creation_tsz']).isoformat()
                day = self.strip(created, self.strip_format).day
                listing_id = k['listing_id']
                month = self.strip(created, self.strip_format).month
                price = float((k['price']))
                paid_est = self.ts(k['paid_tsz']).isoformat()
                product_id = k['product_data']['product_id']
                quantity = k['quantity']
                quarter = ((self.strip(created, self.strip_format).month - 1) // 3) + 1
                receipt_id = k['receipt_id']
                week_num = self.strip(created, self.strip_format).isocalendar()[1]
                year = self.strip(created, self.strip_format).year

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
                product_id, 
                quantity,
                quarter, 
                receipt_id,
                transaction_id,
                week_num,
                year) 
                Values(Now(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
                cur.execute(qry_temp_data, (buyer_user_id,
                                            currency_code,
                                            created,
                                            day,
                                            listing_id,
                                            month,
                                            price,
                                            paid_est,
                                            product_id,
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
            product_id, 
            quantity,
            quarter, 
            receipt_id, 
            transaction_id,
            week_num,
            year)

            SELECT
            SQ1.import_timestamp,
            SQ1.buyer_user_id,
            SQ1.currency_code,
            SQ1.created,
            SQ1.day,
            SQ1.listing_id,
            SQ1.month,
            SQ1.price,
            SQ1.paid_est,
            SQ1.product_id,
            SQ1.quantity,
            SQ1.quarter,
            SQ1.receipt_id,
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
            product_id, 
            quantity,
            quarter, 
            receipt_id, 
            transaction_id,
            week_num,
            year FROM tbl_temp) AS SQ1 LEFT JOIN tbl_etsy_trans ON SQ1.transaction_id = tbl_etsy_trans.transaction_id WHERE tbl_etsy_trans.transaction_id IS NULL;"""
            print("   Inserting New Data Into Table")
            cur.execute(qry_insert_new_data)
            con.commit()
        cur.close()
        con.close()
        print("   Transactions Entered into SQL")

class ListingQuery:
    def __init__(self):
        self.dict = pydict.sql_dict.get
        self.user = self.dict('user')
        self.password = self.dict('password')
        self.host = self.dict('host')
        self.db = self.dict('db_etsy')
        self.dt = datetime
        self.td = timedelta
        self.tz = timezone
        self.strip = self.dt.strptime
        self.strip_format = "%Y-%m-%dT%H:%M:%S"
        self.now = self.dt.now()
        self.ts = self.dt.fromtimestamp

    def listing_id(self):
        print("   Getting Listing IDs from SQL:")
        con = pymysql.connect(user=self.user, password=self.password, host=self.host, database=self.db)
        skus = []
        with con.cursor() as cur:
            query = """SELECT DISTINCT listing_id from tbl_etsy_listings;"""
            cur.execute(query)
            rows = cur.fetchall()
            for row in rows:
                skus.append(row[0])
        return skus

    def listings_from_sql(self):
        print("   def listings_from_sql()")
        con = pymysql.connect(user=self.user, password=self.password, host=self.host, database=self.db)
        data = {}

        with con.cursor() as cur:
            print("   Getting listings from SQL")
            query = """SELECT * FROM etsy.tbl_etsy_listings;"""
            cur.execute(query)
            rows = cur.fetchall()
            headers = cur.description
            h1 = range(len(headers))

            for row in rows:
                temp = {row[0]: {
                    headers[5][0]: row[5],
                    headers[7][0]: row[7],
                    headers[8][0]: row[8],
                    headers[9][0]: row[9],
                    headers[10][0]: row[10],
                    headers[11][0]: row[11],
                    headers[12][0]: row[12],
                    headers[13][0]: row[13],
                    headers[18][0]: row[18],
                    headers[20][0]: row[20],
                    headers[21][0]: row[21],
                    headers[22][0]: row[22],
                    headers[24][0]: row[24],
                    headers[25][0]: row[25],
                    headers[28][0]: row[28],
                    headers[30][0]: row[30],
                    headers[31][0]: row[31],
                    headers[32][0]: row[32],
                    headers[36][0]: row[36],
                    headers[37][0]: row[37]}}
                print(temp)
            con.commit()
        cur.close()
        con.close()

    def listings_active_to_sql(self, data):
        print("   def listings_active_to_sql(data)")
        con = pymysql.connect(user=self.user, password=self.password, host=self.host, database=self.db)

        with con.cursor() as cur:
            print("   Creating Temp Table")
            qry_temp_table = """CREATE TEMPORARY TABLE IF NOT EXISTS tbl_temp like etsy.tbl_etsy_listings;"""
            cur.execute(qry_temp_table)
            con.commit()

            print("   Inserting Data Into Temp Table")
            for k in data.values():
                created_date = self.ts(k['creation_tsz']).isoformat()
                currency_code = k['currency_code']
                ending_date = self.ts(k['ending_tsz']).isoformat()
                featured_rank = k['featured_rank']
                has_variations = k['has_variations']
                is_customizable = k['is_customizable']
                item_dimensions_unit = k['item_dimensions_unit']
                item_height = k['item_height']
                item_length = k['item_length']
                item_weight = k['item_weight']
                item_weight_unit = k['item_weight_unit']
                item_width = k['item_width']
                last_modified_date = self.ts(k['last_modified_tsz']).isoformat()
                listing_id = k['listing_id']
                non_taxable = k['non_taxable']
                num_favorers = k['num_favorers']
                occasion = k['occasion']
                original_created_date = self.ts(k['original_creation_tsz']).isoformat()
                price = k['price']
                processing_max = k['processing_max']
                processing_min = k['processing_min']
                quantity = k['quantity']
                recipient = k['recipient']
                shipping_template_id = k['shipping_template_id']
                shop_section_id = k['shop_section_id']
                should_auto_renew = k['should_auto_renew']
                state = k['state']
                state_tsz = k['state_tsz']
                style = k['style']
                taxonomy_id = k['taxonomy_id']
                title = k['title']
                url = k['url']
                user_id = k['user_id']
                views = k['views']
                when_made = k['when_made']
                who_made = k['who_made']
                tags = ",".join(k['tags'])

                qry_temp_data = """Insert into tbl_temp(import_timestamp,
                    created_date,
                    currency_code,
                    ending_date,
                    featured_rank,
                    has_variations,
                    is_customizable,
                    item_dimensions_unit,
                    item_height,
                    item_length,
                    item_weight,
                    item_weight_unit,
                    item_width,
                    last_modified_date,
                    listing_id,
                    non_taxable,
                    num_favorers,
                    occasion,
                    original_created_date,
                    price,
                    processing_max,
                    processing_min,
                    quantity,
                    recipient,
                    shipping_template_id,
                    shop_section_id,
                    should_auto_renew,
                    state,
                    state_tsz,
                    style,
                    taxonomy_id,
                    title,
                    url,
                    user_id,
                    views,
                    when_made,
                    who_made)
                    Values(Now(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
                cur.execute(qry_temp_data, (created_date,
                                            currency_code,
                                            ending_date,
                                            featured_rank,
                                            has_variations,
                                            is_customizable,
                                            item_dimensions_unit,
                                            item_height,
                                            item_length,
                                            item_weight,
                                            item_weight_unit,
                                            item_width,
                                            last_modified_date,
                                            listing_id,
                                            non_taxable,
                                            num_favorers,
                                            original_created_date,
                                            occasion,
                                            price,
                                            processing_max,
                                            processing_min,
                                            quantity,
                                            recipient,
                                            shipping_template_id,
                                            shop_section_id,
                                            should_auto_renew,
                                            state,
                                            state_tsz,
                                            style,
                                            taxonomy_id,
                                            title,
                                            url,
                                            user_id,
                                            views,
                                            when_made,
                                            who_made
                                            ))
                con.commit()
            print("   Inserting New Data Into Table")
            qry_insert_new_data = """
            INSERT INTO tbl_etsy_listings(
                    import_timestamp,
                    created_date,
                    currency_code,
                    ending_date,
                    featured_rank,
                    has_variations,
                    is_customizable,
                    item_dimensions_unit,
                    item_height,
                    item_length,
                    item_weight,
                    item_weight_unit,
                    item_width,
                    last_modified_date,
                    listing_id,
                    non_taxable,
                    num_favorers,
                    original_created_date,
                    occasion,
                    price,
                    processing_max,
                    processing_min,
                    quantity,
                    recipient,
                    shipping_template_id,
                    shop_section_id,
                    should_auto_renew,
                    state,
                    state_tsz,
                    style,
                    taxonomy_id,
                    title,
                    url,
                    user_id,
                    views,
                    when_made,
                    who_made)

                    SELECT
                    SQ1.import_timestamp,
                    SQ1.created_date,
                    SQ1.currency_code,
                    SQ1.ending_date,
                    SQ1.featured_rank,
                    SQ1.has_variations,
                    SQ1.is_customizable,
                    SQ1.item_dimensions_unit,
                    SQ1.item_height,
                    SQ1.item_length,
                    SQ1.item_weight,
                    SQ1.item_weight_unit,
                    SQ1.item_width,
                    SQ1.last_modified_date,
                    SQ1.listing_id,
                    SQ1.non_taxable,
                    SQ1.num_favorers,
                    SQ1.original_created_date,
                    SQ1.occasion,
                    SQ1.price,
                    SQ1.processing_max,
                    SQ1.processing_min,
                    SQ1.quantity,
                    SQ1.recipient,
                    SQ1.shipping_template_id,
                    SQ1.shop_section_id,
                    SQ1.should_auto_renew,
                    SQ1.state,
                    SQ1.state_tsz,
                    SQ1.style,
                    SQ1.taxonomy_id,
                    SQ1.title,
                    SQ1.url,
                    SQ1.user_id,
                    SQ1.views,
                    SQ1.when_made,
                    SQ1.who_made

                    FROM(SELECT
                    import_timestamp,
                    created_date,
                    currency_code,
                    ending_date,
                    featured_rank,
                    has_variations,
                    is_customizable,
                    item_dimensions_unit,
                    item_height,
                    item_length,
                    item_weight,
                    item_weight_unit,
                    item_width,
                    last_modified_date,
                    listing_id,
                    non_taxable,
                    num_favorers,
                    original_created_date,
                    occasion,
                    price,
                    processing_max,
                    processing_min,
                    quantity,
                    recipient,
                    shipping_template_id,
                    shop_section_id,
                    should_auto_renew,
                    state,
                    state_tsz,
                    style,
                    taxonomy_id,
                    title,
                    url,
                    user_id,
                    views,
                    when_made,
                    who_made
                    FROM tbl_temp) AS SQ1 LEFT JOIN tbl_etsy_listings ON SQ1.listing_id = tbl_etsy_listings.listing_id WHERE tbl_etsy_listings.listing_id IS NULL;"""
            cur.execute(qry_insert_new_data)
            con.commit()
        cur.close()
        con.close()

    def listings_metrics_to_sql(self, data):
        print("   Saving Metrics to SQL")
        con = pymysql.connect(user=self.user, password=self.password, host=self.host, database=self.db)

        with con.cursor() as cur:
            for k in data.values():
                featured_rank = k['featured_rank']
                listing_id = k['listing_id']
                num_favorers = k['num_favorers']
                title = k['title']
                views = k['views']
                qry_insert_data = """Insert into tbl_etsy_metrics(timestamp,
                       featured_rank,
                       listing_id,
                       num_favorers,
                       title,
                       views)
                       Values(Now(), %s, %s, %s, %s, %s);"""
                cur.execute(qry_insert_data, (featured_rank,
                                              listing_id,
                                              num_favorers,
                                              title,
                                              views))
                con.commit()
        cur.close()
        con.close()
        print("Etsy Listing Metrics Process Complete")

class InventoryQuery:
    def __init__(self):
        self.dict = pydict.sql_dict.get
        self.user = self.dict('user')
        self.password = self.dict('password')
        self.host = self.dict('host')
        self.db = self.dict('db_etsy')
        self.dt = datetime
        self.td = timedelta
        self.tz = timezone
        self.strip = self.dt.strptime
        self.strip_format = "%Y-%m-%dT%H:%M:%S"
        self.now = self.dt.now()
        self.ts = self.dt.fromtimestamp

    def inventory_levels(self, data):
        pass



def create_sql_tables():
    ct = CreateTable()
    ct.receipts()
    ct.transactions()
    ct.api_log()
    ct.listings()
    ct.metrics()
create_sql_tables()