from config.walmart_ad_dag_configs import DEFAULT_CONFIGS,search_impression, ad_item, keyword, placement, item_keyword, download_config
import httpx
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from services.request_client import SourceConfig, create_source_client, SourceType
import yaml 
import logging
from utils.common.config_manager import get_cookie_config, update_cookie_config
from services.credential_refresh_service import refresh_walmart_ad_cookies,refresh_headers
import requests
from utils.s3 import S3Hook
import os 
with open('config/credentials.yaml', 'r') as f:
    credentials = yaml.safe_load(f)
TOKEN = credentials['token']

class WalmartAdService:
    def __init__(self):
        self.client = create_source_client(SourceType.WALMART, DEFAULT_CONFIGS['source_config'])
        self.create_job_url = 'http://172.17.2.54:8000/api/v1/walmart/crawl'
        self.base_api_url = 'https://advertising.walmart.com/sp/api/campaigns'
        self.cookies_name = "walmart_ad"


    
    async def refresh_cookies_and_update_config(self) -> bool:
        """Main method to refresh cookies and update configuration using centralized service"""
        return await refresh_walmart_ad_cookies()
    
    async def create_report(self, report_type, **kwargs):
        params = {
            'request': '/v1/snapshot/report',
        }
        
        # Get the config for the report type
        config_map = {
            'searchImpression': search_impression,
            'adItem': ad_item,
            'keyword': keyword,
            'placement': placement,
            'itemKeyword': item_keyword
        }
        
        if report_type not in config_map:
            raise ValueError(f"Invalid report type: {report_type}")
        
        config = config_map[report_type]
        
        # Start with config defaults, then override with provided parameters
        json_data = config.copy()
        
        # Override config defaults with any provided parameters
        for key, value in kwargs.items():
            if key in config:
                json_data[key] = value
        
        # Ensure reportType is set correctly
        json_data['reportType'] = config['reportType']
        
        # Get current PDT time
        pdt_time = datetime.now(ZoneInfo("America/Los_Angeles"))

        # Create a semaphore for the request
        semaphore = asyncio.Semaphore(1)
        logging.info(f"Creating report for {report_type} at {pdt_time}, json_data using {json_data}")
        response, metadata = await self.client.make_request_with_retry(
            url=self.base_api_url, 
            method='POST', 
            params=params, 
            payload=json_data,
            semaphore=semaphore
        )
        if response['response']['code'] == 'failure':
            raise Exception(f"Failed to create report: {response['response']['message']}")
        response['request_time'] = pdt_time.strftime('%Y-%m-%d %H:%M:%S')
        response['reportType'] = report_type
        return response
    
    async def get_report(self):
        params = {
            'request': '/v1/snapshot/report?advertiserId=361947&jobStatus=pending,processing,done',
        }

        # Create a semaphore for the request
        semaphore = asyncio.Semaphore(1)
        response, metadata = await self.client.make_request_with_retry(
            self.base_api_url, 
            method='GET', 
            params=params,
            semaphore=semaphore
        )
        if 'response' not in response:
            logging.error(f"logging session expired, refreshing cookies and update config")
            await self.refresh_cookies_and_update_config()

        reports = response['response']

        return reports
    
    async def save_report_to_s3(self,report_url, cfg):
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
            logging.info(f"✅ Saved report to s3 successfully: {s3_key}")
        else:
            logging.error(f"Failed to save report to s3: {response.status_code}")
            return False
        return True
    



    
