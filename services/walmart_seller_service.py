from httpx._transports.base import A
from config.walmart_ad_dag_configs import DEFAULT_CONFIGS,search_impression, ad_item, keyword, placement, item_keyword, download_config
import httpx
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from services.request_client import SourceConfig, create_source_client, SourceType
import yaml 
import logging
from utils.common.config_manager import get_cookie_config, update_cookie_config
from services.cookie_refresh_service import refresh_walmart_seller_cookies
import requests
from utils.s3 import S3Hook
import os 
from services.header_refresh_serivce import refresh_headers
from config.walmart_seller_dag_configs import DEFAULT_CONFIGS
with open('config/credentials.yaml', 'r') as f:
    credentials = yaml.safe_load(f)
TOKEN = credentials['token']

class WalmartSellerService:
    def __init__(self):
        self.client = create_source_client(SourceType.WALMART, DEFAULT_CONFIGS['source_config'])
        self.base_api_url = 'https://seller.walmart.com/aurora/v1/reports/reportRequests'
        self.cookies_name = "walmart_seller"
        self.credentials_url = 'http://172.17.2.54:8000/api/v1/walmart/credentials?vendor=walmart_seller'



    
    async def refresh_cookies_and_update_config(self) -> bool:
        """Main method to refresh cookies and update configuration using centralized service"""
        return await refresh_walmart_seller_cookies()
    async def on_error_callback(self):
        await refresh_headers(['walmart_seller'], self.credentials_url)
        await refresh_walmart_seller_cookies()
        return True
    
    async def get_total_report_count(self, report_types:list[str]):
        report_types = ','.join(report_types)
        params = {
            "page": 1,
            "limit": 25,
            "reportType": report_types
        }
        semaphore = asyncio.Semaphore(1)
        response, metadata = await self.client.make_request_with_retry(
            url=self.base_api_url, 
            method='GET', 
            params=params,
            semaphore=semaphore,
            on_error=self.on_error_callback
        )
        return {
            "total_count": response['totalCount'],
            'report_types': report_types
        }
    
    async def create_report(self, report_types:list[str], **kwargs):
        report_summary = await self.get_total_report_count(report_types)

        params = {
            "page": 1,
            "limit": report_summary['total_count'],
            "reportType": report_summary['report_types']
        }
        

        # Get current PDT time
        vn_date =   datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date()
        # Create a semaphore for the request
        semaphore = asyncio.Semaphore(1)
        logging.info(f"Creating report for {report_types} at {vn_date}")
        response, metadata = await self.client.make_request_with_retry(
            url=self.base_api_url, 
            method='GET', 
            params=params, 
            semaphore=semaphore,
            on_error=self.on_error_callback
        )
        request_reports = response['requests']
        latest_reports = {}
        for report in request_reports:
            if report['requestStatus'] == 'READY' and datetime.strptime(report['requestSubmissionDate'], "%Y-%m-%dT%H:%M:%SZ").date() == vn_date:
                rtype = report['reportType']
                if rtype not in latest_reports:
                    latest_reports[rtype] = {
                        'report_type': rtype,
                        'report_id': report['requestId'],
                        'report_submission_date': report['requestSubmissionDate']
                    }
                    logging.info(
                        f"Found latest report {rtype} with id {report['requestId']} "
                        f"and submission date {report['requestSubmissionDate']}"
                    )
        
        return list(latest_reports.values())
    
    async def get_report(self, request_id: str):
        params = {
            'requestId': request_id,
        }
        semaphore = asyncio.Semaphore(1)
        response,metadata =  await self.client.make_request_with_retry(
            'https://seller.walmart.com/aurora/v1/reports/downloadReport',
            method='GET',
            params=params,
            semaphore=semaphore,
            on_error=self.on_error_callback
        )
        logging.info(f"Download URL: {response['downloadURL']}")
        return response['downloadURL']
    def _zip_file_to_csv(self, content: str):
        import zipfile
        import csv
        import io
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
                csv_filename = zf.namelist()[0]
                  
                with zf.open(csv_filename) as csv_file:
                    csv_text = csv_file.read().decode("utf-8")  

                    return csv_text 
                        
    def _build_file_name(self, cfg):
        import os 
        # set up for file path
        current_date = datetime.now()
        business_date = datetime.strptime(current_date.strftime("%Y-%m-%d"), "%Y-%m-%d").strftime("%Y%m%d")
        file_name = f"{cfg['prefix']}_{business_date}.csv"   
        os.makedirs(cfg['base_path'], exist_ok=True)
        file_path = os.path.join(cfg['base_path'], file_name)
        return file_path
        
    async def save_report(self, download_url: str, cfg):
        file_path = self._build_file_name(cfg)

        semaphore = asyncio.Semaphore(1)
        response,metadata = await self.client.make_request_with_retry(
            url=download_url,
            method='GET',
            headers = {},
            cookies = {},
            semaphore=semaphore,
            on_error=self.on_error_callback
        )
        csv_data = self._zip_file_to_csv(response['text'])
        with open(file_path, 'w') as f:
            f.write(csv_data)
        logging.info(f"✅ Saved report to {file_path}")
        return file_path
    

    async def update_data_to_sharepoint(self, cfg, chunk_size: int = 500):
        from utils.common.sharepoint.sharepoint_manager import ExcelOnlineLoader
        import pandas as pd
        from datetime import datetime

        file_path = self._build_file_name(cfg)
        df = pd.read_csv(file_path)
        df['date'] = datetime.now().strftime('%Y-%m-%d')

        # Pivot theo sku và date
        new_df = df.pivot_table(
            index="Sku",
            columns="date",
            values="IsSellerBuyBoxWinner",
            aggfunc="first"
        ).reset_index()

        excel_client = ExcelOnlineLoader()

        # Lấy dữ liệu hiện tại trên sheet
        response = excel_client.get_used_range(
            cfg['site_id'], cfg['drive_name'], cfg['file_path'], cfg['sheet_name'], range_address=None
        )
        all_df = pd.DataFrame(response['text'][1:], columns=response['text'][0])

        # Merge để có cột mới
        merged_df = pd.merge(all_df, new_df, on='Sku', how='outer')
        merged_df = merged_df.fillna('')

        row_count, column_count = merged_df.shape
        final_column = excel_client.col_index_to_name(column_count)

        # Gửi header trước (A1:..)
        header_range = f"A1:{final_column}1"
        header_value = {"values": [merged_df.columns.tolist()]}
        excel_client.update_cell(cfg['site_id'], cfg['drive_name'], cfg['file_path'], cfg['sheet_name'], header_range, header_value)

        # Dùng update_cell với chunk_size để tự động chia nhỏ và log lỗi
        full_range = f"A2:{final_column}{row_count}"
        value = {"values": merged_df.values.tolist()}
        result = excel_client.update_cell(
            cfg['site_id'],
            cfg['drive_name'],
            cfg['file_path'],
            cfg['sheet_name'],
            full_range,
            value,
            chunk_size=chunk_size,
        )
        if isinstance(result, dict) and result.get('failed_chunks'):
            for failed in result['failed_chunks']:
                logging.error(f"Failed to update chunk range {failed.get('range')}: {failed.get('error')}")
        else:
            logging.info("All chunks updated successfully")
        pass

    



if __name__ == '__main__':
    import asyncio
    import json
    
    async def main():
        walmart_seller = WalmartSellerService()
        # await walmart_seller.refresh_cookies_and_update_config()
        response = await walmart_seller.create_report(['ITEM'])
        report_id = response[0]['report_id']
        print(report_id)
        # download_url = await walmart_seller.get_report(report_id)
        # download_url = 'https://marketplace.walmartapis.com/v3/reports/getReport/buybox-report/BuyBox_10000001798_2025-09-16T062605.625000.zip?sv=2023-08-03&se=2025-09-16T07%3A38%3A07Z&sr=b&sp=r&sig=7PXb51eiYpsDU1bZPnbR7y54V8cfuFYjoOq4Yc22%2Bhc%3D'

        # report_data = await walmart_seller.save_report(download_url, cfg = cfg)
        # print(report_data)
        # await walmart_seller.update_data_to_sharepoint(cfg)
        pass
    asyncio.run(main())
    pass
    
