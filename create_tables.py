_author_ = 'arichland'

import pymysql
import pydict
sql = pydict.sql_dict.get

def create_tbl_receipts():
    user = sql('user')
    password = sql('password')
    host = sql('host')
    charset = sql('charset')
    cusrorType = pymysql.cursors.DictCursor
    db = sql('db_etsy')
    con = pymysql.connect(user=user, password=password, host=host, database=db, charset=charset, cursorclass=cusrorType)

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

def create_tbl_transactions():
    user = sql('user')
    password = sql('password')
    host = sql('host')
    charset = sql('charset')
    cusrorType = pymysql.cursors.DictCursor
    db = sql('db_etsy')
    con = pymysql.connect(user=user,
                          password=password,
                          host=host,
                          database=db,
                          charset=charset,
                          cursorclass=cusrorType)

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
            quantity INT,
            quarter INT,
            receipt_id BIGINT,
            transaction_id BIGINT,
            week_num INT,
            year INT)
            ENGINE=INNODB;"""
        cur.execute(qry_tbl_transactions)
        con.commit()
    cur.close()
    con.close()

def create_tbl_api_log():
    user = sql('user')
    password = sql('password')
    host = sql('host')
    charset = sql('charset')
    cusrorType = pymysql.cursors.DictCursor
    db = sql('db_rtc')
    con = pymysql.connect(user=user, password=password, host=host, database=db, charset=charset, cursorclass=cusrorType)

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

def create_tbl_listings():
    user = sql('user')
    password = sql('password')
    host = sql('host')
    charset = sql('charset')
    cusrorType = pymysql.cursors.DictCursor
    db = sql('db_etsy')
    con = pymysql.connect(user=user, password=password, host=host, database=db, charset=charset, cursorclass=cusrorType)

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

def create_tbl_etsy_metrics():
    user = sql('user')
    password = sql('password')
    host = sql('host')
    charset = sql('charset')
    cusrorType = pymysql.cursors.DictCursor
    db = sql('db_etsy')
    con = pymysql.connect(user=user, password=password, host=host, database=db, charset=charset, cursorclass=cusrorType)

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


def create_tables():
    create_tbl_receipts()
    create_tbl_transactions()
    create_tbl_api_log()
    create_tbl_listings()
    create_tbl_etsy_metrics()
create_tables()