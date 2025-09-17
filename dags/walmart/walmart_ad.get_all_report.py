import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from airflow.decorators import dag, task
from dags.common.base_init_reports_dag import BaseReportsDAG
from services.walmart_ad_service import WalmartAdService
from config.walmart_ad_dag_configs import search_impression, ad_item, keyword, placement, item_keyword, download_config
from datetime import datetime, timedelta
import asyncio
import logging


class WalmartAdSourceDAG(BaseReportsDAG):
    """Walmart Ad-specific implementation of the base source DAG"""
    
    def __init__(self):
        super().__init__(
            dag_id='walmart_ad.dag_get_all_report_v2',
        )
        self.service = None
    
    def get_service(self):
        """Return the Walmart Ad service instance"""
        if not self.service:
            self.service = WalmartAdService()
        return self.service
    
    def get_report_configs(self, context=None):
        """Return the list of Walmart Ad report configurations"""
        return [search_impression, ad_item, keyword, placement, item_keyword]
        # return [search_impression]
    
    def refresh_credentials(self):
        """Refresh Walmart Ad cookies and update config"""
        service = self.get_service()
        asyncio.run(service.refresh_cookies_and_update_config())
    
    async def create_report(self, report_config):
        """Create a Walmart Ad report and return the response"""
        service = self.get_service()
        end_date = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=16)).strftime("%Y-%m-%d")
        
        return await service.create_report(
            report_type=report_config['reportType'],
            startDate=start_date,
            endDate=end_date
        )
    
    async def get_report_status(self, report_response, report_config):
        """Get the status of a Walmart Ad report"""
        service = self.get_service()
        
        # For Walmart Ad, we need to get all reports and filter by request time
        all_reports = await service.get_report()
        
        if not all_reports:
            return {'status': 'PENDING'}
        
        # Filter reports based on request time for this specific report
        request_time = datetime.fromisoformat(report_response['request_time'])
        time_lower_bound = request_time - timedelta(minutes=1)
        
        matching_reports = [
            r for r in all_reports 
            if r.get('reportType') == report_config['reportType'] and 
               datetime.fromisoformat(r.get("requestedOn")) >= time_lower_bound
        ]
        
        if not matching_reports:
            return {'status': 'PENDING'}
        
        # Get the latest matching report
        latest = sorted(
            matching_reports, 
            key=lambda x: datetime.strptime(x["requestedOn"], "%Y-%m-%d %H:%M:%S"), 
            reverse=True
        )[0]
        
        return {
            'status': latest['jobStatus'],
            'download_url': latest.get('download'),
            'report_data': latest
        }
    
    def is_report_ready(self, status_response):
        """Check if a Walmart Ad report is ready"""
        status = status_response.get('status')
        logging.info(f"Checking if report is ready. Status: {status}, Full response: {status_response}")
        return status == 'done'
    
    async def save_report(self, report_id, report_config, status_response):
        """Save a completed Walmart Ad report"""
        service = self.get_service()
        
        logging.info(f"Saving report {report_config['reportType']} with status: {status_response}")
        
        # Save to S3 using the download config
        await service.save_report_to_s3(
            report_url=status_response['download_url'],
            cfg=download_config[report_config['reportType']]
        )
        

    
    def get_report_identifier(self, report_config):
        """Get a unique identifier for the Walmart Ad report"""
        return report_config['reportType']


# Create the DAG using the @dag decorator
@dag(
    dag_id='walmart_ad.dag_get_all_report',
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 4 * * *",
    catchup=False,
    tags=['walmart_ad']
)
def walmart_ad_report_dag():
    # Get the base DAG tasks
    walmart_ad_dag = WalmartAdSourceDAG()
    refresh_credentials_task, create_all_reports_task, monitor_and_save_reports_task = walmart_ad_dag.create_dag_tasks()
    
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
dag_instance = walmart_ad_report_dag() 