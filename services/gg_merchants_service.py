import asyncio
import logging
import os
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
import yaml
import requests

from services.request_client import create_source_client, SourceType
from services.credential_refresh_service import refresh_gg_merchants_cookies, refresh_headers
from utils.common.config_manager import get_header_config
from utils.s3 import S3Hook
from config.gg_merchants_dag_configs import GG_MERCHANTS_CONFIG
from utils.params_decoder import convert_params_to_dict
from utils.common.job_manager import JobManager

# Constants
class GGMerchantsConstants:
    """Constants for Google Merchants service configuration"""
    COOKIES_NAME = 'gg_merchants'
    CREATE_JOB_URL = 'http://172.17.1.205:8000/api/v1/gg-merchants/crawl'
    CREATE_REPORT_URL = 'https://merchants.google.com/mc_reporting/_/rpc/DownloadService/Download'
    GET_REPORT_URL = 'https://merchants.google.com/mc/download'
    
    # Base payload structure for SKU Visibility Report
    BASE_PAYLOAD = {
        'a': '325260250',
        'f.sid': '',  # Will be filled from params
    }
    
    # Report configurations
    REPORT_CONFIGS = {
        'sku_visibility': {
            '__ar': '{"2":{"1":{"1":{"1":"7807751419","2":"SKU Visibility Report","3":{"1":[{"1":["Date.day","SegmentRawMerchantOfferId.raw_merchant_offer_id","SegmentTitle.title"],"2":[{"1":{"2":"anonymized_all_impressions"},"2":true}],"3":"0","4":"500"},{"1":["anonymized_all_impressions","all_clicks","anonymized_all_events_ctr","conversions","conversion_rate"]}],"4":{"1":28,"2":{"1":"__START_DATE__","2":"__END_DATE__"}},"9":{"1":1}},"4":{"3":[{"1":2}]},"5":{"1":"1751605377627","2":"1757316308941"}},"3":{"4":{"1":2}}}},"3":{"1":"1757317743","2":221000000},"4":4,"5":{"2":"SKU Visibility Report_2025-09-08_00:49:03"}}'
        }
    }

class GGMerchantsService:
    """Service for interacting with Google Merchants API for SKU visibility reports"""
    
    def __init__(self):
        self.client = create_source_client(SourceType.GG_MERCHANTS, GG_MERCHANTS_CONFIG)
        self.create_job_url = GGMerchantsConstants.CREATE_JOB_URL
        self.create_report_url = GGMerchantsConstants.CREATE_REPORT_URL
        self.get_report_url = GGMerchantsConstants.GET_REPORT_URL
        self.cookies_name = GGMerchantsConstants.COOKIES_NAME
        self.s3_hook = S3Hook()

    def _extract_date_components(self, start_date: datetime, end_date: datetime) -> Dict[str, str]:
        """Extract date components for template replacement"""
        return {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d')
        }

    def _replace_date_placeholders(self, template: str, date_components: Dict[str, str]) -> str:
        """Replace date placeholders in the template string"""
        return (
            template
            .replace("__START_DATE__", date_components['start_date'])
            .replace("__END_DATE__", date_components['end_date'])
        )

    def _validate_report_type(self, report_type: str) -> None:
        """Validate that the report type is supported"""
        if report_type not in GGMerchantsConstants.REPORT_CONFIGS:
            raise ValueError(f"Report type '{report_type}' not supported. "
                           f"Supported types: {list(GGMerchantsConstants.REPORT_CONFIGS.keys())}")

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
        payload = GGMerchantsConstants.BASE_PAYLOAD.copy()
        
        # Get report-specific configuration
        report_config = GGMerchantsConstants.REPORT_CONFIGS[report_type]
        
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
            job_id = JobManager().create_job(self.create_job_url, payload={})
            logging.info(f"Created refresh job: {job_id}")
            
            # Poll job until completion
            response = JobManager().poll_job_until_done(job_id)
            
            # Extract credentials URL
            credentials = response['credentials']
            url = next(
                (c['request_url'] for c in credentials if c['api_type'] == 'gg_merchants_create_report'),
                None
            )
            
            if not url:
                raise ValueError("No credentials found for gg_merchants_create_report")
            
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
                ['gg_merchants_create_report', 'gg_merchants_get_report_status'], 
                self.create_job_url,
                payload={"force_new_session": force_new_session}
            )
            await refresh_gg_merchants_cookies()
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
            'authuser': '0',
            'rpcTrackingId': 'DownloadService.Download:1',
            'f.sid': refresh_params['f.sid'],
        }

    async def create_report(self, report_type: str, start_date: datetime, end_date: datetime) -> str:
        """
        Create a Google Merchants SKU visibility report
        
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

            # Update the f.sid in data payload
            data['f.sid'] = refresh_params['f.sid']

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
            if '2' not in response or '1' not in response['2']:
                logging.warning("Report creation failed, retrying with new session")
                await self.on_error_callback(force_new_session=True)
                raise RuntimeError(f"Create report failed with response: {response}")
            
            report_id = response['2']['1']
            logging.info(f"Successfully created report with ID: {report_id}")
            return report_id
            
        except Exception as e:
            logging.error(f"Failed to create report: {e}")
            raise

    def _build_status_params(self, report_id: str) -> Dict[str, str]:
        """Build parameters for report status request"""
        return {
            'a': '325260250',
            'authuser': '0',
            'id': str(report_id),
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
            params = self._build_status_params(report_id)
            semaphore = asyncio.Semaphore(1)
            headers = get_header_config('gg_merchants_get_report_status')

            response, metadata = await self.client.make_request_with_retry(
                self.get_report_url,
                method='GET',
                params=params,
                headers=headers,
                semaphore=semaphore,
                on_error=self.on_error_callback,
            )
            
            # Determine status based on response
            if metadata and metadata['response_status_code'] in [200, 302]:
                status = 'DONE'
                report_url = None  # Google Merchants doesn't provide direct download URL
            else:
                status = 'PENDING'
                report_url = None
            
            logging.info(f"Report {report_id} status: {status}")
            return {
                'status': status,
                'report_url': report_url,
            }
            
        except Exception as e:
            logging.error(f"Failed to get report status for {report_id}: {e}")
            raise

    async def get_report_data(self, report_id: str) -> str:
        """
        Download report data for the given report ID
        
        Args:
            report_id: ID of the report to download
            
        Returns:
            Report data as text
            
        Raises:
            Exception: If download fails
        """
        try:
            params = self._build_status_params(report_id)
            semaphore = asyncio.Semaphore(1)
            headers = get_header_config('gg_merchants_get_report_status')
            
            response, metadata = await self.client.make_request_with_retry(
                self.get_report_url,
                method='GET',
                params=params,
                headers=headers,
                semaphore=semaphore,
                on_error=self.on_error_callback,
            )
            
            logging.info(f"Successfully downloaded report data for {report_id}")
            return response['text']
            
        except Exception as e:
            logging.error(f"Failed to download report data for {report_id}: {e}")
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

