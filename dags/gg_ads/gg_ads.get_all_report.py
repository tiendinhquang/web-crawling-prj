import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from airflow.decorators import dag, task
from dags.common.base_init_reports_dag import BaseReportsDAG
from services.gg_ads_service import GGAdsService
from datetime import datetime, timedelta
import asyncio
import logging

# Report configurations for Google Ads
auction_insights_daily_shopping = {
    'date_range_days': 7,
    'report_type': 'auction_insights_daily_shopping',
    'base_path': 'data/gg_ads',
}

auction_insights_daily_search = {
    'date_range_days': 7,
    'report_type': 'auction_insights_daily_search',
    'base_path': 'data/gg_ads',
}

auction_insights_weekly_shopping = {
    'date_range_days': 30,
    'report_type': 'auction_insights_weekly_shopping',
    'base_path': 'data/gg_ads',
}

auction_insights_weekly_search = {
    'date_range_days': 30,
    'report_type': 'auction_insights_weekly_search',
    'base_path': 'data/gg_ads',
}

auction_insights_monthly_shopping = {
    'date_range_days': 30,
    'report_type': 'auction_insights_monthly_shopping',
    'base_path': 'data/gg_ads',
}

auction_insights_monthly_search = {
    'date_range_days': 30,
    'report_type': 'auction_insights_monthly_search',
    'base_path': 'data/gg_ads',
}

download_config = {
    'auction_insights_daily_shopping': {
        'prefix': 'auction_insights_daily_shopping',
        'base_path': 'data/gg_ads/auction_insights_daily_shopping',
        'bucket_name': 'atlasusa-ap-southeast-1-bi-auto-bucket',
        "bucket_path": "prod/crawler/bronze/gg_ads/auction_insights_daily_shopping/",
    },
    'auction_insights_daily_search': {
        'prefix': 'auction_insights_daily_search',
        'base_path': 'data/gg_ads/auction_insights_daily_search',
        'bucket_name': 'atlasusa-ap-southeast-1-bi-auto-bucket',
        "bucket_path": "prod/crawler/bronze/gg_ads/auction_insights_daily_search/",
    },
    'auction_insights_weekly_shopping': {
        'prefix': 'auction_insights_weekly_shopping',
        'base_path': 'data/gg_ads/auction_insights_weekly_shopping',
        'bucket_name': 'atlasusa-ap-southeast-1-bi-auto-bucket',
        "bucket_path": "prod/crawler/bronze/gg_ads/auction_insights_weekly_shopping/",
    },
    'auction_insights_weekly_search': {
        'prefix': 'auction_insights_weekly_search',
        'base_path': 'data/gg_ads/auction_insights_weekly_search',
        'bucket_name': 'atlasusa-ap-southeast-1-bi-auto-bucket',
        "bucket_path": "prod/crawler/bronze/gg_ads/auction_insights_weekly_search/",
    },
    'auction_insights_monthly_shopping': {
        'prefix': 'auction_insights_monthly_shopping',
        'base_path': 'data/gg_ads/auction_insights_monthly_shopping',
        'bucket_name': 'atlasusa-ap-southeast-1-bi-auto-bucket',
        "bucket_path": "prod/crawler/bronze/gg_ads/auction_insights_monthly_shopping/",
    },
    'auction_insights_monthly_search': {
        'prefix': 'auction_insights_monthly_search',
        'base_path': 'data/gg_ads/auction_insights_monthly_search',
        'bucket_name': 'atlasusa-ap-southeast-1-bi-auto-bucket',
        "bucket_path": "prod/crawler/bronze/gg_ads/auction_insights_monthly_search/",
    },
}

class GGAdsSourceDAG(BaseReportsDAG):
    """Google Ads-specific implementation of the base source DAG"""
    
    def __init__(self):
        super().__init__(
            dag_id='gg_ads.dag_get_all_report',
        )
        self.service = None
    
    def get_service(self):
        """Return the Google Ads service instance"""
        if not self.service:
            self.service = GGAdsService()
        return self.service
    
    def get_report_configs(self, context=None):
        """Return the list of Google Ads report configurations"""
        dag_run = context.get("dag_run") if context else None
        dag_run_conf = dag_run.conf if dag_run else {}
        if dag_run_conf and isinstance(dag_run_conf, dict) and 'reports' in dag_run_conf:
            reports = dag_run_conf['reports']
            if isinstance(reports, list):
                selected_configs = []
                for report_type in reports:
                    if report_type == 'auction_insights_daily_shopping':
                        selected_configs.append(auction_insights_daily_shopping)
                    elif report_type == 'auction_insights_daily_search':
                        selected_configs.append(auction_insights_daily_search)
                    elif report_type == 'auction_insights_weekly_shopping':
                        selected_configs.append(auction_insights_weekly_shopping)
                    elif report_type == 'auction_insights_weekly_search':
                        selected_configs.append(auction_insights_weekly_search)
                    elif report_type == 'auction_insights_monthly_shopping':
                        selected_configs.append(auction_insights_monthly_shopping)
                    elif report_type == 'auction_insights_monthly_search':
                        selected_configs.append(auction_insights_monthly_search)
                return selected_configs
            else:
                logging.warning(f"Invalid reports configuration: {reports}")
                raise ValueError(f"Invalid reports configuration: {reports}")
                
        
        return 
    
    def refresh_credentials(self):
        """Refresh Google Ads cookies and update config"""
        service = self.get_service()
        asyncio.run(service.refresh_cookies_and_update_config())
        
        #
    
    async def create_report(self, report_config):
        """Create a Google Ads report and return the response"""
        service = self.get_service()
        end_date = datetime.now()
        start_date = end_date - timedelta(days=report_config['date_range_days'])
        logging.info(f"Creating report for {report_config['report_type']} from {start_date} to {end_date}")
        # start_date = datetime(2025, 1, 1)
        
        return await service.create_report(
            report_type=report_config['report_type'],
            start_date=start_date,
            end_date=end_date
        )
    
    async def get_report_status(self, report_response, report_config):
        """Get the status of a Google Ads report"""
        service = self.get_service()
        
        # For Google Ads, report_response is the report ID
        status = await service.get_report_status(report_response)
        
        return status
    
    def is_report_ready(self, status_response):
        """Check if a Google Ads report is ready"""
        status = status_response.get('status')
        logging.info(f"Checking if report is ready. Status: {status}, Full response: {status_response}")
        return status == 'DONE'
    
    async def save_report(self, report_id, report_config, status_response):
        """Save a completed Google Ads report"""
        service = self.get_service()
        
        logging.info(f"Saving report {report_config['report_type']} with status: {status_response}")
        
        try:
            # Get the report URL from status response
            report_url = status_response.get('report_url')
            if not report_url:
                raise ValueError(f"No report URL found in status response: {status_response}")
            
            # Download the report data
            report_data = await service.get_report_data(report_url)
            
            # Prepare S3 upload details
            bucket_path = download_config[report_config['report_type']]['bucket_path']
            prefix = download_config[report_config['report_type']]['prefix']
            s3_key = f"{bucket_path}/{prefix}_{datetime.now().strftime('%Y%m%d')}.csv"
            
            # Save to S3
            await service.save_report_to_s3(
                report_type=report_config['report_type'],
                report_data=report_data,
                base_path=download_config[report_config['report_type']]['base_path'],
                s3_key=s3_key,
                process_date=datetime.now(),
                bucket_name=download_config[report_config['report_type']]['bucket_name']
            )
            
            return s3_key
            
        except Exception as e:
            logging.error(f"Error saving report {report_config['report_type']}: {str(e)}")
            raise
    
    def get_report_identifier(self, report_config):
        """Get a unique identifier for the Google Ads report"""
        return report_config['report_type']

# Create the DAG using the @dag decorator
@dag(
    dag_id='gg_ads.dag_get_all_report',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['gg_ads']
)
def gg_ads_report_dag():
    # Get the base DAG tasks
    gg_ads_dag = GGAdsSourceDAG()
    refresh_credentials_task, create_all_reports_task, monitor_and_save_reports_task = gg_ads_dag.create_dag_tasks()
    
    # Execute the tasks
    credentials_refreshed = refresh_credentials_task()
    reports_data = create_all_reports_task()
    final_results = monitor_and_save_reports_task(reports_data)
    
    # Set dependencies
    credentials_refreshed >> reports_data >> final_results
    
    return {
        'credentials_refreshed': credentials_refreshed,
        'reports_created': reports_data,
        'reports_completed': final_results
    }

# Create the DAG instance
dag_instance = gg_ads_report_dag()
