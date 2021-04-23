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
    def listings_from_sql():
        print("   Saving New Receipts to SQL:")
        user = pydict.sql_dict.get('user')
        password = pydict.sql_dict.get('password')
        host = pydict.sql_dict.get('host')
        charset = pydict.sql_dict.get('charset')
        db = pydict.sql_dict.get('db_etsy')
        con = pymysql.connect(user=user, password=password, host=host, database=db, charset=charset)
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

    def listings_active_to_sql(data):
        print("   Saving New Receipts to SQL:")
        user = pydict.sql_dict.get('user')
        password = pydict.sql_dict.get('password')
        host = pydict.sql_dict.get('host')
        charset = pydict.sql_dict.get('charset')
        db = pydict.sql_dict.get('db_etsy')
        con = pymysql.connect(user=user, password=password, host=host, database=db, charset=charset)

        with con.cursor() as cur:
            print("   Creating Temp Table")
            qry_temp_table = """CREATE TEMPORARY TABLE IF NOT EXISTS tbl_temp like etsy.tbl_etsy_listings;"""
            cur.execute(qry_temp_table)
            con.commit()

            print("   Inserting Data Into Temp Table")
            for k in data.values():
                created_date = ts(k['creation_tsz']).isoformat()
                currency_code = k['currency_code']
                ending_date = ts(k['ending_tsz']).isoformat()
                featured_rank = k['featured_rank']
                has_variations = k['has_variations']
                is_customizable = k['is_customizable']
                item_dimensions_unit = k['item_dimensions_unit']
                item_height = k['item_height']
                item_length = k['item_length']
                item_weight = k['item_weight']
                item_weight_unit = k['item_weight_unit']
                item_width = k['item_width']
                last_modified_date = ts(k['last_modified_tsz']).isoformat()
                listing_id = k['listing_id']
                non_taxable = k['non_taxable']
                num_favorers = k['num_favorers']
                occasion = k['occasion']
                original_created_date = ts(k['original_creation_tsz']).isoformat()
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

    def listings_metrics_to_sql(data):
        print("   Saving Metrics to SQL")
        user = pydict.sql_dict.get('user')
        password = pydict.sql_dict.get('password')
        host = pydict.sql_dict.get('host')
        charset = pydict.sql_dict.get('charset')
        db = pydict.sql_dict.get('db_etsy')
        con = pymysql.connect(user=user, password=password, host=host, database=db, charset=charset)

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

class listing_functions:
    def update_listing():
        base = pydict.etsy_urls.get('base')
        url = base+"/listing/"

        limit = 50
        offset = 0
        filter = ''
        time = ''
        endpoint = "Update Listings"
        params = {"limit": limit,
                  "offset": offset}
        method = "Get"
        data = etsy.api.call_etsy(method, url, params, limit, endpoint)
        #for k in data.values():
        #    pp.pprint(k)
        #sql.listings_active(data)

    def listings():
        url = pydict.etsy_urls.get('shop_listings')
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
            tags = k['tags']
            ",".join(tags)
            print(",".join(tags))
        #sql.listings_active_to_sql(data)

    def products():
        url = pydict.etsy_urls.get('listing_product')
        limit = 50
        offset = 0
        filter = ''
        time = ''
        endpoint = "All Active Listings"
        params = {"limit": limit,
                  "offset": offset}
        method = "Get"
        data = etsy.api.call_etsy(method, url, params, limit, endpoint)
        pp.pprint(data)

    def metrics():
        print("\nRetrieving Etsy Listing Metrics")
        url = pydict.etsy_urls.get('shop_listings')
        limit = 50
        offset = 0
        filter = ''
        time = etsy.api.update_time()
        endpoint = "Listings Metrics"
        params = {"limit": limit,
                  "offset": offset}
        method = "Get"
        data = etsy.api.call_etsy(method, url, params, limit, endpoint)
        pp.pprint(data)
        #sql.listings_metrics_to_sql(data)

listing_functions.products()
