import httpx
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from services.request_client import SourceConfig, create_source_client, SourceType
import yaml 
import logging
from utils.common.config_manager import get_cookie_config, update_cookie_config, get_header_config
from services.cookie_refresh_service import refresh_gg_merchants_cookies
from services.header_refresh_serivce import refresh_headers
import requests
from utils.s3 import S3Hook
import os 
from config.gg_merchants_dag_configs import GG_MERCHANTS_CONFIG
from utils.params_decoder import convert_params_to_dict
with open('config/credentials.yaml', 'r') as f:
    credentials = yaml.safe_load(f)
TOKEN = credentials['token']

class GGMerchantsService:
    def __init__(self):
        self.client = create_source_client(SourceType.GG_MERCHANTS, GG_MERCHANTS_CONFIG)
        self.credentials_url = 'http://172.17.2.54:8000/api/v1/gg-merchants/credentials'
        self.create_report_url = 'https://merchants.google.com/mc_reporting/_/rpc/DownloadService/Download'
        self.get_report_url = 'https://merchants.google.com/mc/download'
        self.cookies_name = 'gg_merchants'
        self.s3_hook = S3Hook()

    async def refresh_cookies_and_update_config(self) -> bool:
        """Main method to refresh cookies and update configuration using centralized service"""
        await refresh_headers(['gg_merchants_create_report', 'gg_merchants_get_report_status'], self.credentials_url)
        await refresh_gg_merchants_cookies()
        return True
    
    async def get_params(self):
        headers = {
            'Authorization': f'Bearer {TOKEN}'
        }
        response = requests.get(self.credentials_url, headers=headers)
        response_data = response.json()['credentials']['credentials']
        url  = next(c['request_url'] for c in response_data if c['api_type'] == 'gg_merchants_create_report')
        params = convert_params_to_dict(url)
        logging.info(f"Get params success: {params}")
        return params
    async def create_sku_visibility_report(self,start_date:datetime,end_date:datetime):
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        params = await self.get_params()
        params = {
            'authuser': '0',
            'rpcTrackingId': 'DownloadService.Download:1',
            'f.sid': params['f.sid'],
        }

        data = {
            'a': '325260250',
            'f.sid': params['f.sid'],
            '__ar': '{"2":{"1":{"1":{"1":"7807751419","2":"SKU Visibility Report","3":{"1":[{"1":["Date.day","SegmentRawMerchantOfferId.raw_merchant_offer_id","SegmentTitle.title"],"2":[{"1":{"2":"anonymized_all_impressions"},"2":true}],"3":"0","4":"500"},{"1":["anonymized_all_impressions","all_clicks","anonymized_all_events_ctr","conversions","conversion_rate"]}],"4":{"1":28,"2":{"1":"__START_DATE__","2":"__END_DATE__"}},"9":{"1":1}},"4":{"3":[{"1":2}]},"5":{"1":"1751605377627","2":"1757316308941"}},"3":{"4":{"1":2}}}},"3":{"1":"1757317743","2":221000000},"4":4,"5":{"2":"SKU Visibility Report_2025-09-08_00:49:03"}}',
        }

        # thay thế trực tiếp
        data["__ar"] = (
            data["__ar"]
            .replace("__START_DATE__", start_date_str)
            .replace("__END_DATE__", end_date_str)

        )

        semaphore = asyncio.Semaphore(1)
        response, metadata = await self.client.make_request_with_retry(
            self.create_report_url,
            method='POST',
            params=params,
            data=data,
            semaphore=semaphore,
            on_error=self.refresh_cookies_and_update_config
        )
        id = response['2']['1']
        logging.info(f"Create report success with id: {id}, date range from {start_date_str} to {end_date_str}")
        return id

    async def get_report_status(self,id:str):
        params = {
            'a': '325260250',
            'authuser': '0',
            'id': str(id),
        }


        semaphore = asyncio.Semaphore(1)
        headers = get_header_config('gg_merchants_get_report_status')

        response, metadata = await self.client.make_request_with_retry(
            self.get_report_url,
            method='GET',
            params=params,
            headers=headers,
            semaphore=semaphore,
            on_error=self.refresh_cookies_and_update_config,
       
        )
        
        if metadata and metadata['response_status_code'] in [200, 302]:
            status = 'DONE'
        else:
            status = 'PENDING'
        return status

    async def get_report_data(self,id:str):
        params = {
            'a': '325260250',
            'authuser': '0',
            'id': str(id),
        }

        semaphore = asyncio.Semaphore(1)
        headers = get_header_config('gg_merchants_get_report_status')
        response,metadata = await self.client.make_request_with_retry(
            self.get_report_url,
            method='GET',
            params=params,
            headers=headers,
            semaphore=semaphore,
            on_error=self.refresh_cookies_and_update_config,
        )
        
        # The response should contain the actual report data
        # Check if it's a text response or binary
        return response['text']

    async def save_report_to_s3(self, report_type: str, report_data,base_path:str,s3_key:str, process_date: datetime, bucket_name:str):
        file_name = f"{report_type}_{process_date.strftime('%Y%m%d')}.csv"
        file_path = os.path.join(base_path, file_name)
        
        os.makedirs(base_path, exist_ok=True)
        
        with open(file_path, 'w') as f:
            f.write(report_data)
        self.s3_hook.load_file(
            filename=file_path,
            key=s3_key,
            replace=True,
            bucket_name=bucket_name
        )
        logging.info(f"Report save to local: {file_path}")
        logging.info(f"Report saved to S3: {s3_key}")

if __name__ == "__main__":
    import asyncio
    async def main():
        gg_merchants_service = GGMerchantsService()
        start_date = datetime(2025, 8, 22)
        end_date = datetime.now()
        # await gg_merchants_service.refresh_cookies_and_update_config()
        # id = await gg_merchants_service.create_sku_visibility_report(start_date, end_date)
        id = '1757925564382527961'
        data = await gg_merchants_service.get_report_status(id)
        # print(data)
        # data = await gg_merchants_service.get_report_data(id)
        print(data)
    asyncio.run(main())
    

    
    