import httpx
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from services.request_client import SourceConfig, create_source_client, SourceType
import yaml 
import logging
from utils.common.config_manager import get_cookie_config, update_cookie_config, get_header_config
from services.credential_refresh_service import refresh_lowes_vendor_cookies
import requests
from utils.s3 import S3Hook
import os 
from config.lowes_vendor_dag_configs import LOWES_RTM_CONFIG


class LowesVendorService:
    def __init__(self):
        self.client = create_source_client(SourceType.LOWES, LOWES_RTM_CONFIG)
        self.create_job_url = 'http://172.17.1.205:8000/api/v1/lowes/crawl'
        self.base_api_url = 'https://vendorgateway.lowes.com/vendorinquiry/vendorinquiry-api'

            
    async def refresh_cookies_and_update_config(self) -> bool:
        """Main method to refresh cookies and update configuration using centralized service"""
        return await refresh_lowes_vendor_cookies()

    async def get_rtm_deductions_list(self, start_date, end_date):
        url = f"{self.base_api_url}/deduction/findRTMDeductionList"
        payload = {
            'vendorId': '89026',
            'deductionDateFrom': start_date.strftime("%Y-%m-%d"),
            'deductionDateTo': end_date.strftime("%Y-%m-%d"),
            'deductionStatus': '4',
        }
        semaphore = asyncio.Semaphore(1)
        response, metadata = await self.client.make_request_with_retry(url, method='POST', payload=payload, semaphore=semaphore, on_error=self.refresh_cookies_and_update_config)
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


