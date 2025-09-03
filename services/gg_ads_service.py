import httpx
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from services.request_client import SourceConfig, create_source_client, SourceType
import yaml 
import logging
from utils.common.config_manager import get_cookie_config, update_cookie_config, get_header_config
from services.cookie_refresh_service import refresh_gg_ads_cookies
import requests
from utils.s3 import S3Hook
import os 
from config.gg_ads_dag_configs import GG_ADS_CONFIG
with open('config/credentials.yaml', 'r') as f:
    credentials = yaml.safe_load(f)
TOKEN = credentials['token']

class GGAdsService:
    def __init__(self):
        self.client = create_source_client(SourceType.GG_ADS, GG_ADS_CONFIG)
        self.crawler_bearer_token = TOKEN
        self.cookies_url = 'http://172.17.2.54:8000/api/v1/gg-ads/cookies'
        self.params_url = 'http://172.17.2.54:8000/api/v1/gg-ads/params'
        self.create_report_url = 'https://ads.google.com/aw_reporting/dashboard/_/rpc/ReportDownloadService/DownloadReport'
        self.get_report_status_url = 'https://ads.google.com/aw_reporting/dashboard/_/rpc/ReportDownloadService/GetState'
        self.cookies_name = 'gg_ads'
        self.s3_hook = S3Hook()


    def refresh_cookies_and_update_config(self) -> bool:
        """Main method to refresh cookies and update configuration using centralized service"""
        return refresh_gg_ads_cookies()

    def _build_request_payload(self, report_type: str, start_date: datetime, end_date: datetime) -> dict:
        """
        Build the request payload based on report configuration
        
        Args:
            report_type: Type of report to generate
            start_date: Start date for the report
            end_date: End date for the report
            
        Returns:
            Dictionary containing the request payload
        """
        st_year, st_month, st_date = start_date.year, start_date.month, start_date.day
        ed_year, ed_month, ed_date = end_date.year, end_date.month, end_date.day
        
        # Base payload structure
        base_payload = {
            'hl': 'en_US',
            '__lu': '1443558140',
            '__u': '3493560860',
            '__c': '9212818423',
            'f.sid': '-5148428733118097000',
            'ps': 'aw',
            'activityContext': 'DownloadMenu.QuickDownload.CSV',
            'requestPriority': 'HIGH_LATENCY_SENSITIVE',
            'activityType': 'USER_NON_BLOCKING',
            'activityId': '990627585237302',
            'uniqueFingerprint': '-5918933209572725000_990627585237302_1',
            'destinationPlace': '/aw/insights/auctioninsights',
        }
        
        # Report-specific configurations
        report_configs = {
            'auction_insights_daily_shopping': {
                '__ar': '{"1":[{"2":{"1":"Auction insights report","2":2,"3":1,"4":true,"5":true,"6":false,"7":[{"1":{"1":{"3":{"1":"641764527"},"5":"TABLE","6":"-25200000","7":"1612280132994","8":"{\\"1\\":[{\\"1\\":\\"1000004\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000033\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000049\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000050\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000052\\",\\"2\\":\\"CONTROL\\"},{\\"1\\":\\"1000053\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000055\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000056\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000063\\",\\"2\\":\\"CONTROL\\"}]}"},"2":{"1":["segmentation_info.day","merchant_display_name","stats.shopping_impression_share","stats.shopping_overlap_rate","stats.shopping_outranking_share"],"4":{"1":{"1":__ST_YEAR__,"2":__ST_MONTH__,"3":__ST_DATE__},"2":{"1":__ED_YEAR__,"2":__ED_MONTH__,"3":__ED_DATE__}},"10":true,"14":false},"3":[{"1":"INVERT_GROUPING_ENTITIES","2":"TRUE"},{"1":"PUSH_CUSTOMER_FILTER_INTO_LABEL_SUBVIEW","2":"TRUE"},{"1":"PUSH_ID_FILTERS_INTO_LABEL_SUBVIEW","2":"TRUE"},{"1":"UI_VISIBLE_FIELDS","2":"segmentation_info.day,merchant_display_name,stats.shopping_impression_share,stats.shopping_overlap_rate,stats.shopping_outranking_share"}]},"2":44}],"16":false,"18":"/aw/insights/auctioninsights","20":"641764527","28":true}}],"2":2,"3":true,"10":true,"12":false,"14":1,"16":{"1":"641764527","2":"641764527"},"20":3}'
            
            },
            'auction_insights_daily_search': {
                '__ar': '{"1":[{"2":{"1":"Auction insights report","2":2,"3":1,"4":true,"5":true,"6":false,"7":[{"1":{"1":{"3":{"1":"641764527"},"5":"TABLE","6":"-25200000","7":"1612280132994","8":"{\\"1\\":[{\\"1\\":\\"1000004\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000033\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000049\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000050\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000052\\",\\"2\\":\\"CONTROL\\"},{\\"1\\":\\"1000053\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000055\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000056\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000063\\",\\"2\\":\\"CONTROL\\"}]}"},"2":{"1":[segmentation_info.day,"domain","stats.impression_share","stats.average_position_for_auction","stats.overlap_rate","stats.competitor_above_rate","stats.promoted_rate","stats.absolute_top_of_page_rate","stats.outranking_share"],"4":{"1":{"1":__ST_YEAR__,"2":__ST_MONTH__,"3":__ST_DATE__},"2":{"1":__ED_YEAR__,"2":__ED_MONTH__,"3":__ED_DATE__}},"14":false},"3":[{"1":"INVERT_GROUPING_ENTITIES","2":"TRUE"},{"1":"PUSH_CUSTOMER_FILTER_INTO_LABEL_SUBVIEW","2":"TRUE"},{"1":"PUSH_ID_FILTERS_INTO_LABEL_SUBVIEW","2":"TRUE"},{"1":"UI_VISIBLE_FIELDS","2":"segmentation_info.day,domain,stats.impression_share,stats.average_position_for_auction,stats.overlap_rate,stats.competitor_above_rate,stats.promoted_rate,stats.absolute_top_of_page_rate,stats.outranking_share"}]},"2":36}],"16":false,"18":"/aw/insights/auctioninsights","20":"641764527","28":true}}],"2":2,"3":true,"10":true,"12":false,"14":1,"16":{"1":"641764527","2":"641764527"},"20":3}'
           
            },
            'auction_insights_weekly_shopping': {
                '__ar': '{"1":[{"2":{"1":"Auction insights report","2":2,"3":1,"4":true,"5":true,"6":false,"7":[{"1":{"1":{"3":{"1":"641764527"},"5":"TABLE","6":"-25200000","7":"1612280132994","8":"{\\"1\\":[{\\"1\\":\\"1000004\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000033\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000049\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000050\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000052\\",\\"2\\":\\"CONTROL\\"},{\\"1\\":\\"1000053\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000055\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000056\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000063\\",\\"2\\":\\"CONTROL\\"}]}"},"2":{"1":["segmentation_info.week","merchant_display_name","stats.shopping_impression_share","stats.shopping_overlap_rate","stats.shopping_outranking_share"],"4":{"1":{"1":__ST_YEAR__,"2":__ST_MONTH__,"3":__ST_DATE__},"2":{"1":__ED_YEAR__,"2":__ED_MONTH__,"3":__ED_DATE__}},"10":true,"14":false},"3":[{"1":"INVERT_GROUPING_ENTITIES","2":"TRUE"},{"1":"PUSH_CUSTOMER_FILTER_INTO_LABEL_SUBVIEW","2":"TRUE"},{"1":"PUSH_ID_FILTERS_INTO_LABEL_SUBVIEW","2":"TRUE"},{"1":"UI_VISIBLE_FIELDS","2":"segmentation_info.week,merchant_display_name,stats.shopping_impression_share,stats.shopping_overlap_rate,stats.shopping_outranking_share"}]},"2":44}],"16":false,"18":"/aw/insights/auctioninsights","20":"641764527","28":true}}],"2":2,"3":true,"10":true,"12":false,"14":1,"16":{"1":"641764527","2":"641764527"},"20":3}'
            },
            'auction_insights_weekly_search': {
                '__ar': '{"1":[{"2":{"1":"Auction insights report","2":2,"3":1,"4":true,"5":true,"6":false,"7":[{"1":{"1":{"3":{"1":"641764527"},"5":"TABLE","6":"-25200000","7":"1612280132994","8":"{\\"1\\":[{\\"1\\":\\"1000004\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000033\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000049\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000050\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000052\\",\\"2\\":\\"CONTROL\\"},{\\"1\\":\\"1000053\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000055\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000056\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000063\\",\\"2\\":\\"CONTROL\\"}]}"},"2":{"1":["segmentation_info.week","domain","stats.impression_share","stats.average_position_for_auction","stats.overlap_rate","stats.competitor_above_rate","stats.promoted_rate","stats.absolute_top_of_page_rate","stats.outranking_share"],"4":{"1":{"1":__ST_YEAR__,"2":__ST_MONTH__,"3":__ST_DATE__},"2":{"1":__ED_YEAR__,"2":__ED_MONTH__,"3":__ED_DATE__}},"10":true,"14":false},"3":[{"1":"INVERT_GROUPING_ENTITIES","2":"TRUE"},{"1":"PUSH_CUSTOMER_FILTER_INTO_LABEL_SUBVIEW","2":"TRUE"},{"1":"PUSH_ID_FILTERS_INTO_LABEL_SUBVIEW","2":"TRUE"},{"1":"UI_VISIBLE_FIELDS","2":"segmentation_info.week,domain,stats.impression_share,stats.average_position_for_auction,stats.overlap_rate,stats.competitor_above_rate,stats.promoted_rate,stats.absolute_top_of_page_rate,stats.outranking_share"}]},"2":36}],"16":false,"18":"/aw/insights/auctioninsights","20":"641764527","28":true}}],"2":2,"3":true,"10":true,"12":false,"14":1,"16":{"1":"641764527","2":"641764527"},"20":3}'
            },

            'auction_insights_monthly_shopping': {
               '__ar': '{"1":[{"2":{"1":"Auction insights report","2":2,"3":1,"4":true,"5":true,"6":false,"7":[{"1":{"1":{"3":{"1":"641764527"},"5":"TABLE","6":"-25200000","7":"1612280132994","8":"{\\"1\\":[{\\"1\\":\\"1000004\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000033\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000049\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000050\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000052\\",\\"2\\":\\"CONTROL\\"},{\\"1\\":\\"1000053\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000055\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000056\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000063\\",\\"2\\":\\"CONTROL\\"}]}"},"2":{"1":["segmentation_info.month","merchant_display_name","stats.shopping_impression_share","stats.shopping_overlap_rate","stats.shopping_outranking_share"],"4":{"1":{"1":__ST_YEAR__,"2":__ST_MONTH__,"3":__ST_DATE__},"2":{"1":__ED_YEAR__,"2":__ED_MONTH__,"3":__ED_DATE__}},"10":true,"14":false},"3":[{"1":"INVERT_GROUPING_ENTITIES","2":"TRUE"},{"1":"PUSH_CUSTOMER_FILTER_INTO_LABEL_SUBVIEW","2":"TRUE"},{"1":"PUSH_ID_FILTERS_INTO_LABEL_SUBVIEW","2":"TRUE"},{"1":"UI_VISIBLE_FIELDS","2":"segmentation_info.month,merchant_display_name,stats.shopping_impression_share,stats.shopping_overlap_rate,stats.shopping_outranking_share"}]},"2":44}],"16":false,"18":"/aw/insights/auctioninsights","20":"641764527","28":true}}],"2":2,"3":true,"10":true,"12":false,"14":1,"16":{"1":"641764527","2":"641764527"},"20":3}'
            },
            'auction_insights_monthly_search': {
                
                '__ar': '{"1":[{"2":{"1":"Auction insights report","2":2,"3":1,"4":true,"5":true,"6":false,"7":[{"1":{"1":{"3":{"1":"641764527"},"5":"TABLE","6":"-25200000","7":"1612280132994","8":"{\\"1\\":[{\\"1\\":\\"1000004\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000033\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000049\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000050\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000052\\",\\"2\\":\\"CONTROL\\"},{\\"1\\":\\"1000053\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000055\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000056\\",\\"2\\":\\"TREATMENT\\"},{\\"1\\":\\"1000063\\",\\"2\\":\\"CONTROL\\"}]}"},"2":{"1":["segmentation_info.month","domain","stats.impression_share","stats.average_position_for_auction","stats.overlap_rate","stats.competitor_above_rate","stats.promoted_rate","stats.absolute_top_of_page_rate","stats.outranking_share"],"4":{"1":{"1":__ST_YEAR__,"2":__ST_MONTH__,"3":__ST_DATE__},"2":{"1":__ED_YEAR__,"2":__ED_MONTH__,"3":__ED_DATE__}},"10":true,"14":false},"3":[{"1":"INVERT_GROUPING_ENTITIES","2":"TRUE"},{"1":"PUSH_CUSTOMER_FILTER_INTO_LABEL_SUBVIEW","2":"TRUE"},{"1":"PUSH_ID_FILTERS_INTO_LABEL_SUBVIEW","2":"TRUE"},{"1":"UI_VISIBLE_FIELDS","2":"segmentation_info.month,domain,stats.impression_share,stats.average_position_for_auction,stats.overlap_rate,stats.competitor_above_rate,stats.promoted_rate,stats.absolute_top_of_page_rate,stats.outranking_share"}]},"2":36}],"16":false,"18":"/aw/insights/auctioninsights","20":"641764527","28":true}}],"2":2,"3":true,"10":true,"12":false,"14":1,"16":{"1":"641764527","2":"641764527"},"20":3}'
            },
           
        }
        
        if report_type not in report_configs:
            raise ValueError(f"Report type {report_type} not supported")
        
        # Get the report-specific configuration
        report_config = report_configs[report_type]
        
        # Replace date placeholders in the __ar field
        if '__ar' in report_config:
            ar_template = report_config['__ar']
            ar_with_dates = (
                ar_template
                .replace("__ST_YEAR__", str(st_year))
                .replace("__ST_MONTH__", str(st_month))
                .replace("__ST_DATE__", str(st_date))
                .replace("__ED_YEAR__", str(ed_year))
                .replace("__ED_MONTH__", str(ed_month))
                .replace("__ED_DATE__", str(ed_date))
            )
            base_payload['__ar'] = ar_with_dates
        
        return base_payload
    async def get_refresh_params(self):
        semaphore = asyncio.Semaphore(1)
        response, metadata = await self.client.make_request_with_retry(
            self.params_url,
            method='GET',
            semaphore=semaphore,
            headers=  {
                'Authorization': f'Bearer {self.crawler_bearer_token}'
            }
        )
        return response['params']
    
    async def create_report(self, report_type:str ,start_date:datetime, end_date:datetime):
        # Build the request payload using the new method
        data = self._build_request_payload(report_type, start_date, end_date)
        refresh_params = await self.get_refresh_params()
        params = {
            'authuser': '2',
            'xt': 'awn',
            'acx-v-bv': refresh_params['acx-v-bv'],
            'acx-v-clt': refresh_params['acx-v-clt'],
            'rpcTrackingId': 'ReportDownloadService.DownloadReport:3',
            'f.sid': refresh_params['f.sid'],
        }


        semaphore = asyncio.Semaphore(1)
        response, metadata = await self.client.make_request_with_retry(
            self.create_report_url,
            method='POST',
            params=params,
            data=data,
            semaphore=semaphore,
        )
        
        #{"1":"1055136338"}
        if '1' not in response:
            raise Exception(f"Create report failed with response: {response}")
        id = response['1']
        logging.info(f"Create report success with id: {id}")
        return id

    async def get_report_status(self,id:str):
        refresh_params = await self.get_refresh_params()
        params = {
            'authuser': '2',
            'xt': 'awn',
            'acx-v-bv': refresh_params['acx-v-bv'],
            'acx-v-clt': refresh_params['acx-v-clt'],
            'rpcTrackingId': 'ReportDownloadService.GetState:5',
            'f.sid': refresh_params['f.sid'],
        }
        ar_template = '{"1":"__ID__","2":{"3":{"1":"641764527"}}}'
        ar_str = ar_template.replace("__ID__", id)
        data = {
            'hl': 'en_US',
            '__lu': '1443558140',
            '__u': '3493560860',
            '__c': '9212818423',
            'f.sid': '-5148428733118097000',
            'ps': 'aw',
            '__ar': ar_str,
            'activityContext': 'DownloadMenu.QuickDownload.CSV',
            'requestPriority': 'HIGH_LATENCY_SENSITIVE',
            'activityType': 'USER_NON_BLOCKING',
            'activityId': '2451650259271362',
            'uniqueFingerprint': '-5148428733118097000_2451650259271362_2',
            'destinationPlace': '/aw/insights/auctioninsights',
        }

        semaphore = asyncio.Semaphore(1)
        headers = get_header_config('gg_ads_get_report_status')
        response, metadata = await self.client.make_request_with_retry(
            self.get_report_status_url,
            params=params,
            data=data,
            semaphore=semaphore,
            headers=headers,
        )
        
        report_url = response.get('1').get('4', None)
        status = 'PENDING'
        if report_url:
            status = 'DONE'
        return {
            'status': status,
            'report_url': report_url,
        }

    async def get_report_data(self,report_url:str):
        semaphore = asyncio.Semaphore(1)
        response,metadata = await self.client.make_request_with_retry(
            report_url,
            method='GET',
            semaphore=semaphore,
            headers = {},
            cookies = {},
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
        gg_ads_service = GGAdsService()
        start_date = datetime(2025, 8, 22)
        end_date = datetime.now()
        # gg_ads_service.refresh_cookies_and_update_config()
        # report_type = 'auction_insights_daily_shopping'
        # id = await gg_ads_service.create_report(report_type ,start_date, end_date)
        # id = '1055340276'
        # response = await gg_ads_service.get_report_status(id)
        # print(response)
        # report_url = 'https://storage.googleapis.com/awn-report-download/download/1055340276/Auction%20insights%20report.csv?GoogleAccessId=816718982741-compute@developer.gserviceaccount.com&Expires=1756264278&response-content-disposition=attachment&Signature=rDutHAtM%2FNl3MnIFoaF7qZm%2FpIfLKVuVk5FajV8k8c3r98L9nlk02Ql8dXxP1t1UVXaHO747cU7CzsmwcIVbEMgTUokhv%2FwX1CwsoVdqd%2BFMCi6DvFBD1YaWsnMhqCz0%2FN7iDcBSBQzq6R2gQzN376if%2BXKyz8SWGhwpJTbl37fumZAJqUKle3fr9YrXkz8PAYUuhj2POXgZUchKCFMCnvmkDfCkyI4j52hie%2Fw37qd6kVvbCwYv%2FaK8ezYpzJ1F4VFDj%2FN1cKDhQzp1c60eClP10yU17%2BUXwePk8AVPmhRLGTerze%2Fv2fCFEC%2FGArsLdCLx5cBH4d1Vb0%2BOCo68Lw%3D%3D'
        # response = await gg_ads_service.get_report_data(report_url)
        # print(response)
        refresh_params = await gg_ads_service.get_refresh_params()
        print(refresh_params)
    asyncio.run(main())
    
