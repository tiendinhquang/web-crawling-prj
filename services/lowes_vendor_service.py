import httpx
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from services.request_client import SourceConfig, create_source_client, SourceType
import yaml 
import logging
from utils.common.config_manager import get_cookie_config, update_cookie_config, get_header_config
from services.cookie_refresh_service import refresh_lowes_vendor_cookies
import requests
from utils.s3 import S3Hook
import os 
from config.lowes_vendor_dag_configs import LOWES_RTM_CONFIG
with open('config/credentials.yaml', 'r') as f:
    credentials = yaml.safe_load(f)
TOKEN = credentials['token']

class LowesVendorService:
    def __init__(self):
        self.client = create_source_client(SourceType.LOWES, LOWES_RTM_CONFIG)
        self.cookies_url = 'http://172.17.2.54:8000/api/v1/lowes/cookies'
        self.base_api_url = 'https://vendorgateway.lowes.com/vendorinquiry/vendorinquiry-api'
        self.cookies_name = 'lowes_vendor'

            
    def refresh_cookies_and_update_config(self) -> bool:
        """Main method to refresh cookies and update configuration using centralized service"""
        return refresh_lowes_vendor_cookies()

    async def get_rtm_deductions_list(self, start_date, end_date):
        url = f"{self.base_api_url}/deduction/findRTMDeductionList"
        payload = {
            'vendorId': '89026',
            'deductionDateFrom': start_date.strftime("%Y-%m-%d"),
            'deductionDateTo': end_date.strftime("%Y-%m-%d"),
            'deductionStatus': '4',
        }
        semaphore = asyncio.Semaphore(1)
        response, metadata = await self.client.make_request_with_retry(url, method='POST', payload=payload, semaphore=semaphore)
        deductions = response["data"]
        logging.info(f"Number of deductions Processed: {len(deductions)}")
        return deductions
    def get_process_rtm_deductions(self, process_date):
        year,month,day = process_date.year, process_date.month, process_date.day
        base_path = f'data/lowes/{year}/{month}/{day}/rtm'
        if not os.path.exists(base_path):
            return []
        tracking_numbers = [file.split('_')[0] for file in os.listdir(base_path)]
        return tracking_numbers
            
    async def save_vendor_report_to_s3(self, report_url, cfg):
        http_client = httpx.AsyncClient()
        response = await http_client.get(report_url)
        current_date = datetime.now()
        business_date = datetime.strptime(current_date.strftime("%Y-%m-%d"), "%Y-%m-%d").strftime("%Y%m%d")
        if response.status_code == 200:
            report_data = response.content
            file_name = f"{cfg['prefix']}_{business_date}.csv"   
            os.makedirs(cfg['base_path'], exist_ok=True)
            file_path = os.path.join(cfg['base_path'], file_name)
            with open(file_path, 'wb') as f:
                f.write(report_data)
            s3_key = os.path.join(cfg['bucket_path'], file_name).replace("\\", "/")
            s3_hook = S3Hook()
            s3_hook.load_file(
                filename=file_path,
                key=s3_key,
                bucket_name=cfg['bucket_name'],
                replace=True,
            )
            logging.info(f"✅ Saved vendor report to s3 successfully: {s3_key}")
        else:
            logging.error(f"Failed to save vendor report to s3: {response.status_code}")
            return False
        return True


if __name__ == "__main__":
    import asyncio
    async def main():
        lowes_vendor_service = LowesVendorService()
        start_date = datetime(2025, 6, 22)
        end_date = datetime.now()
        lowes_vendor_service.refresh_cookies_and_update_config()
        deductions = await lowes_vendor_service.get_rtm_deductions_list(start_date, end_date)
        # print(deductions)
    asyncio.run(main())
    
    # json_data = {'vendorId': 89026, 'vendorName': 'ATLAS INTERNATIONAL INC       ', 'updatedOn': '2025-08-20', 'updatedBy': 'DTCB054 ', 'invoiceNumber': None, 'storeNumber': 0, 'source': 'RTM', 'status': None, 'deductionStatus': None, 'trackingNumber': 'LOWRTM016959961', 'deductionNumber': 'DM9040DA', 'deductionDate': '2025-08-20', 'sellingLocation': None, 'deductionAmount': -3308.25, 'approvedAmount': 0, 'debitBackupSent': None, 'dmrlLowesComments': None, 'dmrlPONumber': None}
    # url  ="https://vendorgateway.lowes.com/vendorinquiry/vendorinquiry-api/deduction/findRTMDeductionDetails"
    # headers = get_header_config('lowes_vendor')
    # cookies = get_cookie_config('lowes_vendor')
    # response = requests.post(url,headers=headers,cookies=cookies, json=json_data)
    # print(response.json())
    # print(deductions)