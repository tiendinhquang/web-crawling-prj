from utils.common.config_manager import get_cookie_config, get_header_config
import requests

headers = get_header_config('gg_merchants')
all_cookies = get_cookie_config('gg_merchants')
cookies_key = ['SEARCH_SAMESITE', 'OGPC', 'OGP', 'MCN_CLTKN', 'SID', '__Secure-1PSID', '__Secure-3PSID', 'HSID', 'SSID', 'APISID', 'SAPISID', '__Secure-1PAPISID', '__Secure-3PAPISID', 'ADS_VISITOR_ID', 'AEC', 'NID', '__Secure-1PSIDTS', '__Secure-3PSIDTS', 'SIDCC', '__Secure-1PSIDCC', '__Secure-3PSIDCC']
cookies = {}
for k,v in all_cookies.items():
    if k in cookies_key:
        cookies[k] = v


params = {
    'authuser': '2',
    'rpcTrackingId': 'DownloadService.Download:4',
    'f.sid': '-7443189923069850000',
}

data = {
    'a': '325260250',
    'f.sid': '-7443189923069850000',
    '__ar': '{"2":{"1":{"1":{"1":"7807751419","2":"SKU Visibility Report","3":{"1":[{"1":["Date.day","SegmentRawMerchantOfferId.raw_merchant_offer_id","SegmentTitle.title"],"2":[{"1":{"2":"anonymized_all_impressions"},"2":true}],"3":"0","4":"200"},{"1":["anonymized_all_impressions","all_clicks","anonymized_all_events_ctr","conversions","conversion_rate"]}],"4":{"1":28,"2":{"1":"2025-08-01","2":"2025-08-07"}},"9":{"1":1}},"4":{"3":[{"1":2}]},"5":{"1":"1751605377627","2":"1755067720632"}},"3":{"4":{"1":2}}}},"3":{"1":"1755853388","2":943000000},"4":4,"5":{"2":"SKU Visibility Report_2025-08-22_02:03:08"}}',
}

response = requests.post(
    'https://merchants.google.com/mc_reporting/_/rpc/DownloadService/Download',
    params=params,
    cookies=cookies,
    headers=headers,
    data=data,
)

pass