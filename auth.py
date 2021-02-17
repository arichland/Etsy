_author_ = 'arichland'

from requests_oauthlib import OAuth1Session
import webbrowser
import pprint
from etsy2.oauth import EtsyOAuthHelper, EtsyOAuthClient
import urllib.parse as urlparse
from urllib.parse import parse_qs
import pydict
pp = pprint.PrettyPrinter(indent=1)

api = pydict.new_app_api_auth.get
user_id = api('oauth_user_id')
client_key = api('oauth_consumer_key')
client_secret = api('client_secret')
permission_scopes = api('permission_scopes')
request_token = api('request_token_url')
access_token_url = api('token_url')
api_verifier = api('verifier')
request_token_secret = api('request_token_secret')


request_token_url = request_token
url = 'https://openapi.etsy.com/v2/oauth/request_token?'
token_url = access_token_url
verifier = api_verifier

login_url, temp_oauth_token_secret = \
    EtsyOAuthHelper.get_request_url_and_token_secret(client_key, client_secret, permission_scopes)

oauth = OAuth1Session(client_key=client_key, client_secret=client_secret, signature_type=u'AUTH_HEADER')
response = oauth.fetch_request_token(request_token_url)

webbrowser.open_new(login_url)

query = urlparse.urlparse(login_url).query
temp_oauth_token = parse_qs(query)['oauth_token'][0]
print(login_url)


oauth_token, oauth_token_secret = \
    EtsyOAuthHelper.get_oauth_token_via_verifier(client_key, client_secret, temp_oauth_token, temp_oauth_token_secret, input('Verifier: '))

etsy_oauth = EtsyOAuthClient(client_key=client_key,
                            client_secret=client_secret,
                            resource_owner_key=oauth_token,
                            resource_owner_secret=oauth_token_secret)

print(oauth_token)
print(oauth_token_secret)