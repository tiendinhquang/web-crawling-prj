import asyncio
import logging
import os
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
import yaml

from services.request_client import create_source_client, SourceType
from services.credential_refresh_service import refresh_gg_ads_cookies, refresh_headers,BaseRefreshService
from utils.common.config_manager import get_header_config
from utils.s3 import S3Hook
from config.gg_ads_dag_configs import GG_ADS_CONFIG
from utils.params_decoder import convert_params_to_dict
from utils.common.job_manager import JobManager

# Constants
class GGAdsConstants:
    """Constants for Google Ads service configuration"""
    COOKIES_NAME = 'gg_ads'
    CREATE_JOB_URL = 'http://172.17.1.205:8000/api/v1/gg-ads/crawl'
    CREATE_REPORT_URL = 'https://ads.google.com/aw_reporting/dashboard/_/rpc/ReportDownloadService/DownloadReport'
    GET_REPORT_STATUS_URL = 'https://ads.google.com/aw_reporting/dashboard/_/rpc/ReportDownloadService/GetState'
    GET_JOB_URL = 'http://172.17.1.205:8000/api/v1/jobs'
    
    # Base payload structure
    BASE_PAYLOAD = {
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
    
    # Report configurations
    REPORT_CONFIGS = {
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
        }
    }

class GGAdsService:
    """Service for interacting with Google Ads API for auction insights reports"""
    
    def __init__(self):
        self.client = create_source_client(SourceType.GG_ADS, GG_ADS_CONFIG)
        self.create_job_url = GGAdsConstants.CREATE_JOB_URL
        self.create_report_url = GGAdsConstants.CREATE_REPORT_URL
        self.get_report_status_url = GGAdsConstants.GET_REPORT_STATUS_URL
        self.get_job_url = GGAdsConstants.GET_JOB_URL
        self.cookies_name = GGAdsConstants.COOKIES_NAME
        self.s3_hook = S3Hook()




    def _extract_date_components(self, start_date: datetime, end_date: datetime) -> Dict[str, int]:
        """Extract date components for template replacement"""
        return {
            'st_year': start_date.year,
            'st_month': start_date.month,
            'st_date': start_date.day,
            'ed_year': end_date.year,
            'ed_month': end_date.month,
            'ed_date': end_date.day
        }

    def _replace_date_placeholders(self, template: str, date_components: Dict[str, int]) -> str:
        """Replace date placeholders in the template string"""
        return (
            template
            .replace("__ST_YEAR__", str(date_components['st_year']))
            .replace("__ST_MONTH__", str(date_components['st_month']))
            .replace("__ST_DATE__", str(date_components['st_date']))
            .replace("__ED_YEAR__", str(date_components['ed_year']))
            .replace("__ED_MONTH__", str(date_components['ed_month']))
            .replace("__ED_DATE__", str(date_components['ed_date']))
        )

    def _validate_report_type(self, report_type: str) -> None:
        """Validate that the report type is supported"""
        if report_type not in GGAdsConstants.REPORT_CONFIGS:
            raise ValueError(f"Report type '{report_type}' not supported. "
                           f"Supported types: {list(GGAdsConstants.REPORT_CONFIGS.keys())}")

    def _build_request_payload(self, report_type: str, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """
        Build the request payload based on report configuration
        
        Args:
            report_type: Type of report to generate
            start_date: Start date for the report
            end_date: End date for the report
            
        Returns:
            Dictionary containing the request payload
            
        Raises:
            ValueError: If report_type is not supported
        """
        self._validate_report_type(report_type)
        
        # Extract date components
        date_components = self._extract_date_components(start_date, end_date)
        
        # Start with base payload
        payload = GGAdsConstants.BASE_PAYLOAD.copy()
        
        # Get report-specific configuration
        report_config = GGAdsConstants.REPORT_CONFIGS[report_type]
        
        # Replace date placeholders in the __ar field
        if '__ar' in report_config:
            ar_template = report_config['__ar']
            ar_with_dates = self._replace_date_placeholders(ar_template, date_components)
            payload['__ar'] = ar_with_dates
        
        return payload

    async def get_refresh_params(self) -> Dict[str, str]:
        """
        Get refresh parameters by creating and polling a job
        
        Returns:
            Dictionary containing refresh parameters
            
        Raises:
            ValueError: If job creation fails
            RuntimeError: If job execution fails
            TimeoutError: If job doesn't complete in time
        """
        semaphore = asyncio.Semaphore(1)
        
        try:
            # Create job
            job_id = JobManager().create_job(self.create_job_url, payload= {})
            logging.info(f"Created refresh job: {job_id}")
            
            # Poll job until completion
            response = JobManager().poll_job_until_done(job_id)
            
            # Extract credentials URL
            credentials = response['credentials']
            url = next(
                (c['request_url'] for c in credentials if c['api_type'] == 'gg_ads_auction_insights'),
                None
            )
            
            if not url:
                raise ValueError("No credentials found for gg_ads_auction_insights")
            
            params = convert_params_to_dict(url)
            logging.info("Successfully retrieved refresh parameters")
            return params
            
        except Exception as e:
            logging.error(f"Failed to get refresh params: {e}")
            raise

    async def refresh_cookies_and_update_config(self, force_new_session: bool = False) -> bool:
        """
        Refresh cookies and update configuration
        
        Args:
            force_new_session: Whether to force a new session
            
        Returns:
            True if successful
            
        Raises:
            Exception: If refresh fails
        """
        try:
            await refresh_headers(
                ['headers:gg_ads_auction_insights'], 
                self.create_job_url, 
                payload={"force_new_session": force_new_session}
            )
            await refresh_gg_ads_cookies()
            logging.info("Successfully refreshed cookies and updated config")
            return True
        except Exception as e:
            logging.error(f"Failed to refresh cookies and update config: {e}")
            raise

    async def on_error_callback(self, force_new_session: bool = False) -> None:
        """
        Error callback to refresh credentials and parameters
        
        Args:
            force_new_session: Whether to force a new session
        """
        try:
            await self.refresh_cookies_and_update_config(force_new_session)
            await self.get_refresh_params()
            logging.info("✅ Error callback completed successfully")
        except Exception as e:
            logging.error(f"Error in on_error_callback: {e}")
            raise



    def _build_report_params(self, refresh_params: Dict[str, str]) -> Dict[str, str]:
        """Build parameters for report creation request"""
        return {
            'authuser': '2',
            'xt': 'awn',
            'acx-v-bv': refresh_params['acx-v-bv'],
            'acx-v-clt': refresh_params['acx-v-clt'],
            'rpcTrackingId': 'ReportDownloadService.DownloadReport:3',
            'f.sid': refresh_params['f.sid'],
        }

    async def create_report(self, report_type: str, start_date: datetime, end_date: datetime) -> str:
        """
        Create a Google Ads auction insights report
        
        Args:
            report_type: Type of report to generate
            start_date: Start date for the report
            end_date: End date for the report
            
        Returns:
            Report ID string
            
        Raises:
            ValueError: If report type is invalid
            RuntimeError: If report creation fails
        """
        try:
            # Build the request payload
            data = self._build_request_payload(report_type, start_date, end_date)
            refresh_params = await self.get_refresh_params()
            params = self._build_report_params(refresh_params)

            semaphore = asyncio.Semaphore(1)
            response, metadata = await self.client.make_request_with_retry(
                self.create_report_url,
                method='POST',
                params=params,
                data=data,
                semaphore=semaphore,
                on_error=self.on_error_callback,
            )
            
            # Check if report creation was successful
            if '1' not in response:
                logging.warning("Report creation failed, retrying with new session")
                await self.on_error_callback(force_new_session=True)
                raise RuntimeError(f"Create report failed with response: {response}")
            
            report_id = response['1']
            logging.info(f"Successfully created report with ID: {report_id}")
            return report_id
            
        except Exception as e:
            logging.error(f"Failed to create report: {e}")
            raise

    def _build_status_params(self, refresh_params: Dict[str, str]) -> Dict[str, str]:
        """Build parameters for report status request"""
        return {
            'authuser': '2',
            'xt': 'awn',
            'acx-v-bv': refresh_params['acx-v-bv'],
            'acx-v-clt': refresh_params['acx-v-clt'],
            'rpcTrackingId': 'ReportDownloadService.GetState:5',
            'f.sid': refresh_params['f.sid'],
        }

    def _build_status_data(self, report_id: str) -> Dict[str, Any]:
        """Build data payload for report status request"""
        ar_template = '{"1":"__ID__","2":{"3":{"1":"641764527"}}}'
        ar_str = ar_template.replace("__ID__", report_id)
        
        return {
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

    async def get_report_status(self, report_id: str) -> Dict[str, Optional[str]]:
        """
        Get the status of a report generation
        
        Args:
            report_id: The ID of the report to check
            
        Returns:
            Dictionary containing status and report_url
            
        Raises:
            Exception: If status check fails
        """
        try:
            refresh_params = await self.get_refresh_params()
            params = self._build_status_params(refresh_params)
            data = self._build_status_data(report_id)

            semaphore = asyncio.Semaphore(1)
            headers = get_header_config('gg_ads_auction_insights')
            
            response, metadata = await self.client.make_request_with_retry(
                self.get_report_status_url,
                params=params,
                data=data,
                semaphore=semaphore,
                headers=headers,
                on_error=self.on_error_callback,
            )
            
            # Extract report URL from response
            report_url = None
            if response.get('1') and response['1'].get('4'):
                report_url = response['1']['4']
            
            status = 'DONE' if report_url else 'PENDING'
            
            logging.info(f"Report {report_id} status: {status}")
            return {
                'status': status,
                'report_url': report_url,
            }
            
        except Exception as e:
            logging.error(f"Failed to get report status for {report_id}: {e}")
            raise

    async def get_report_data(self, report_url: str) -> str:
        """
        Download report data from the given URL
        
        Args:
            report_url: URL to download the report from
            
        Returns:
            Report data as text
            
        Raises:
            Exception: If download fails
        """
        try:
            semaphore = asyncio.Semaphore(1)
            response, metadata = await self.client.make_request_with_retry(
                report_url,
                method='GET',
                semaphore=semaphore,
                headers={},
                cookies={},
                on_error=self.on_error_callback,
            )
            
            logging.info(f"Successfully downloaded report data from {report_url}")
            return response['text']
            
        except Exception as e:
            logging.error(f"Failed to download report data from {report_url}: {e}")
            raise

    async def save_report_to_s3(self, report_type: str, report_data: str, base_path: str, 
                               s3_key: str, process_date: datetime, bucket_name: str) -> None:
        """
        Save report data to local file and upload to S3
        
        Args:
            report_type: Type of the report
            report_data: The report data to save
            base_path: Local base path for the file
            s3_key: S3 key for the file
            process_date: Date for the report
            bucket_name: S3 bucket name
            
        Raises:
            Exception: If save operation fails
        """
        try:
            file_name = f"{report_type}_{process_date.strftime('%Y%m%d')}.csv"
            file_path = os.path.join(base_path, file_name)
            
            # Create directory if it doesn't exist
            os.makedirs(base_path, exist_ok=True)
            
            # Write data to local file
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(report_data)
            
            # Upload to S3
            self.s3_hook.load_file(
                filename=file_path,
                key=s3_key,
                replace=True,
                bucket_name=bucket_name
            )
            
            logging.info(f"Report saved locally: {file_path}")
            logging.info(f"Report uploaded to S3: {s3_key}")
            
        except Exception as e:
            logging.error(f"Failed to save report to S3: {e}")
            raise

if __name__ == "__main__":
    import asyncio
    gg_ads_service = GGAdsService()
    # async def main():
        
        # start_date = datetime(2025, 8, 22)
        # end_date = datetime.now()
        # await gg_ads_service.refresh_cookies_and_update_config()
        # report_type = 'auction_insights_daily_shopping'
        # id = await gg_ads_service.create_report( 'auction_insights_daily_shopping' ,start_date, end_date)
        # id = '1061563134'
        # response = await gg_ads_service.get_report_status(id)
        # print(response)
        # report_url = 'https://storage.googleapis.com/awn-report-download/download/1055340276/Auction%20insights%20report.csv?GoogleAccessId=816718982741-compute@developer.gserviceaccount.com&Expires=1756264278&response-content-disposition=attachment&Signature=rDutHAtM%2FNl3MnIFoaF7qZm%2FpIfLKVuVk5FajV8k8c3r98L9nlk02Ql8dXxP1t1UVXaHO747cU7CzsmwcIVbEMgTUokhv%2FwX1CwsoVdqd%2BFMCi6DvFBD1YaWsnMhqCz0%2FN7iDcBSBQzq6R2gQzN376if%2BXKyz8SWGhwpJTbl37fumZAJqUKle3fr9YrXkz8PAYUuhj2POXgZUchKCFMCnvmkDfCkyI4j52hie%2Fw37qd6kVvbCwYv%2FaK8ezYpzJ1F4VFDj%2FN1cKDhQzp1c60eClP10yU17%2BUXwePk8AVPmhRLGTerze%2Fv2fCFEC%2FGArsLdCLx5cBH4d1Vb0%2BOCo68Lw%3D%3D'
        # response = await gg_ads_service.get_report_data(report_url)
        # print(response)
        # refresh_params = await gg_ads_service.get_refresh_params()
        # print(refresh_params)
    # asyncio.run(main())
    
    


    from concurrent.futures import ThreadPoolExecutor

    # Giả sử on_error_callback là hàm đồng bộ
    def test():
        asyncio.run(gg_ads_service.on_error_callback(force_new_session=True))

    async def main():
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(max_workers=3) as pool:
            # submit 3 tasks vào thread pool
            tasks = [
                loop.run_in_executor(pool, test),
                # loop.run_in_executor(pool, test),
                # loop.run_in_executor(pool, test),
            ]
            await asyncio.gather(*tasks)

    asyncio.run(main())
