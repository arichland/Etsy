_author_ = 'arichland'

sql_dict = {
    'user': '',
    'password': '',
    'host': '',
    'charset': '',
    'db_etsy': 'etsy'
}
api_auth = {
    'api_key': '',
    'shared_secret': '',
    'request_token_url': 'https://openapi.etsy.com/v2/oauth/request_token?scope=email_r&listings_w&listings_d,transactions_r&transactions_w&address_w&feedback_r',
    'token_url': 'https://openapi.etsy.com/v2/oauth/access_token',
    'verifier': '',
    'user_id': '',
    'shop_id': '',
    'oauth_user_id': '',
    'verify': '',
    'token_secret': '',
    'request_token_secret': '',
    'oauth_consumer_key': '',
    'client_secret': '',
    'oauth_token': '',
    'oauth_signature': '',
    'oauth_nonce': '',
    'url': 'https://openapi.etsy.com/v2/shops/20978352/transactions?'
}
etsy_data_url= {
    'new_receipts': 'https://openapi.etsy.com/v2/shops/20978352/receipts?findAllShopReceipts&limit=100&min_last_modified=%s',
    'hist_recepts': ["https://openapi.etsy.com/v2/shops/20978352/receipts?&limit=100&offset=0",
                     "https://openapi.etsy.com/v2/shops/20978352/receipts?&limit=100&offset=100",
                     "https://openapi.etsy.com/v2/shops/20978352/receipts?&limit=100&offset=200",
                     "https://openapi.etsy.com/v2/shops/20978352/receipts?&limit=100&offset=300",
                     "https://openapi.etsy.com/v2/shops/20978352/receipts?&limit=100&offset=400"],
    'new_trans': 'https://openapi.etsy.com/v2/shops/20978352/transactions?findAllListingTransactions&limit=100&min_last_modified=%s',
    'hist_trans': ["https://openapi.etsy.com/v2/shops/20978352/transactions?limit=100&offset=0",
                   "https://openapi.etsy.com/v2/shops/20978352/transactions?limit=100&offset=100",
                   "https://openapi.etsy.com/v2/shops/20978352/transactions?limit=100&offset=200",
                   "https://openapi.etsy.com/v2/shops/20978352/transactions?limit=100&offset=300"]
}

params = ['shop_id', 'was_paid', 'listings_d', 'transactions_r', 'transactions_w', 'address_w', 'feedback_r']
permission_scopes = ['email_r', 'listings_w', 'listings_d', 'transactions_r', 'transactions_w', 'address_w', 'feedback_r']

api_header = {'Authorization': 'OAuth oauth_consumer_key="",'
                            'oauth_token="",'
                            'oauth_signature_method="PLAINTEXT",'
                            'oauth_timestamp="",'
                            'oauth_nonce="",'
                            'oauth_version="1.0",'
                            'oauth_signature=""'}
api_code_dict = {
    'Etsy': {
        200: 'Success',
        201: 'Created',
        400: 'Bad Request',
        403: 'Forbidden',
        404: 'Not Found',
        500: 'Server Error',
        503: 'Service Unavailable'}}