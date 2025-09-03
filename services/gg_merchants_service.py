import httpx
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from services.request_client import SourceConfig, create_source_client, SourceType
import yaml 
import logging
from utils.common.config_manager import get_cookie_config, update_cookie_config, get_header_config
from services.cookie_refresh_service import refresh_gg_merchants_cookies
import requests
from utils.s3 import S3Hook
import os 
from config.gg_merchants_dag_configs import GG_MERCHANTS_CONFIG
with open('config/credentials.yaml', 'r') as f:
    credentials = yaml.safe_load(f)
TOKEN = credentials['token']

class GGMerchantsService:
    def __init__(self):
        self.client = create_source_client(SourceType.GG_MERCHANTS, GG_MERCHANTS_CONFIG)
        self.cookies_url = 'http://172.17.2.54:8000/api/v1/gg-merchants/cookies'
        self.params_url = 'http://172.17.2.54:8000/api/v1/gg-merchants/params'
        self.create_report_url = 'https://merchants.google.com/mc_reporting/_/rpc/DownloadService/Download'
        self.get_report_url = 'https://merchants.google.com/mc/download'
        self.cookies_name = 'gg_merchants'
        self.s3_hook = S3Hook()

    def refresh_cookies_and_update_config(self) -> bool:
        """Main method to refresh cookies and update configuration using centralized service"""
        return refresh_gg_merchants_cookies()
    async def get_params(self):
        headers = {
            'Authorization': f'Bearer {TOKEN}'
        }
        response = requests.get(self.params_url, headers=headers)
        return response.json()['params']
    async def create_sku_visibility_report(self,start_date:datetime,end_date:datetime):
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        params = await self.get_params()

        data = {
            'a': '325260250',
            'f.sid': params['f.sid'],
            '__ar': '{"2":{"1":{"1":{"1":"7807751419","2":"SKU Visibility Report","3":{"1":[{"1":["Date.day","SegmentRawMerchantOfferId.raw_merchant_offer_id","SegmentTitle.title"],"2":[{"1":{"2":"anonymized_all_impressions"},"2":true}],"3":"0","4":"500"},{"1":["anonymized_all_impressions","all_clicks","anonymized_all_events_ctr","conversions","conversion_rate"]}],"4":{"1":28,"2":{"1":"__START_DATE__","2":"__END_DATE__"}},"9":{"1":1}},"4":{"3":[{"1":2}]},"5":{"1":"1751605377627","2":"1756721600891"}},"3":{"4":{"1":2}}}},"3":{"1":"1756722307","2":569000000},"4":4,"5":{"2":"SKU Visibility Report_2025-09-01_03:25:07"}}',
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
            semaphore=semaphore
        )
        id = response['2']['1']
        logging.info(f"Create report success with id: {id}, date range from {start_date_str} to {end_date_str}")
        return id

    async def get_report_status(self,id:str):
        params = {
            'authuser': '0',
            'rpcTrackingId': 'DownloadService.Download:1',
            'f.sid': '-1242487546587814700',
        }


        data = {
            'a': '325260250',
            'f.sid': '5044951462939783000',
            '__ar': '{"2":{"2":[{"1":"type","2":3,"3":[{"2":"7"}]}]}}',
        }

        semaphore = asyncio.Semaphore(1)
        headers = get_header_config('gg_merchants_get_report_status')
        response, metadata = await self.client.make_request_with_retry(
            'https://merchants.google.com/mc/_/rpc/AlertService/List',
            params=params,
            data=data,
            semaphore=semaphore,
            headers=headers,
       
        )
        
        ids = [str(item["3"]["4"]["1"]["1"]) for item in response["1"]]
        if id in ids:
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
        headers = get_header_config('gg_merchants_get_report')
        response,metadata = await self.client.make_request_with_retry(
            self.get_report_url,
            method='GET',
            params=params,
            headers=headers,
            semaphore=semaphore,

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
        # gg_merchants_service.refresh_cookies_and_update_config()
        # id = await gg_merchants_service.create_sku_visibility_report(start_date, end_date)
        id = '1756721603580342766'
        # data = await gg_merchants_service.get_report_data(id)
        # print(data)
        status = await gg_merchants_service.get_report_status(id)
        print(status)
    asyncio.run(main())
    
    # json_data = {'vendorId': 89026, 'vendorName': 'ATLAS INTERNATIONAL INC       ', 'updatedOn': '2025-08-20', 'updatedBy': 'DTCB054 ', 'invoiceNumber': None, 'storeNumber': 0, 'source': 'RTM', 'status': None, 'deductionStatus': None, 'trackingNumber': 'LOWRTM016959961', 'deductionNumber': 'DM9040DA', 'deductionDate': '2025-08-20', 'sellingLocation': None, 'deductionAmount': -3308.25, 'approvedAmount': 0, 'debitBackupSent': None, 'dmrlLowesComments': None, 'dmrlPONumber': None}
    # url  ="https://vendorgateway.lowes.com/vendorinquiry/vendorinquiry-api/deduction/findRTMDeductionDetails"
    # headers = get_header_config('lowes_vendor')
    # cookies = get_cookie_config('lowes_vendor')
    # response = requests.post(url,headers=headers,cookies=cookies, json=json_data)
    # print(response.json())
    # print(deductions)
    
    
    