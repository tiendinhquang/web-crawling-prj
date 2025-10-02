import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from airflow.decorators import dag, task
from dags.common.base_init_reports_dag import BaseReportsDAG
from services.walmart_seller_service import WalmartSellerService
from datetime import datetime, timedelta
import asyncio
import logging

from config.walmart_seller_report_configs import BUYBOX_CONFIG


class WalmartSellerSourceDAG(BaseReportsDAG):
    """Walmart Seller-specific implementation of the base source DAG"""
    
    def __init__(self):
        super().__init__(
            dag_id='walmart_seller.dag_get_all_report',
        )
        self.service = None
    
    def get_service(self):
        """Return the Walmart Seller service instance"""
        if not self.service:
            self.service = WalmartSellerService()
        return self.service
    
    def get_report_configs(self, context=None):
        """Return the list of Walmart Seller report configurations"""
        # Use centralized configs so adding new reports is simple
        return [BUYBOX_CONFIG]
    
    def refresh_credentials(self):
        """Refresh Walmart Seller cookies and update config"""
        service = self.get_service()
        asyncio.run(service.on_error_callback())
    
    async def create_report(self, report_config):
        """For Seller Center, fetch latest READY report for given type and return requestId"""
        service = self.get_service()
        # For Seller Center, create_report() lists READY reports; we pick the one for today
        latest_reports = await service.create_report(report_types=report_config['report_types'])
        if not latest_reports:
            raise Exception(f"No READY reports found for types: {report_config['report_types']}")
        # We expect one per type since we pass a single type list
        return latest_reports[0]['report_id']
    
    async def get_report_status(self, report_id, report_config):
        """Get direct download URL for a Seller Center report; treat as ready if available"""
        service = self.get_service()
        try:
            download_url = await service.get_report(report_id)
            if download_url:
                return {'status': 'done', 'download_url': download_url}
            return {'status': 'PENDING'}
        except Exception as e:
            logging.error(f"Failed to get report download URL for {report_id}: {e}")
            return {'status': 'PENDING', 'error': str(e)}
    
    def is_report_ready(self, status_response):
        """Check if a Seller Center report is ready"""
        status = status_response.get('status')
        logging.info(f"Checking if report is ready. Status: {status}, Full response: {status_response}")
        return status == 'done' or bool(status_response.get('download_url'))
    
    async def save_report(self, report_id, report_config, status_response):
        """Save a completed Seller Center report and return local file path"""
        service = self.get_service()
        logging.info(f"Saving report {report_config['name']} with status: {status_response}")
        download_url = status_response['download_url']
        cfg = report_config['cfg']
        return await service.save_report(download_url=download_url, cfg=cfg)

    
    def get_report_identifier(self, report_config):
        """Get a unique identifier for the Walmart Seller report"""
        return report_config.get('name') or ','.join(report_config.get('report_types', []))
    
    def sync_data_to_sharepoint(self, report_config):
        """Sync data to Sharepoint"""
        service = self.get_service()
        cfg = report_config['cfg']
        return asyncio.run(service.update_data_to_sharepoint(cfg=cfg))


# Create the DAG using the @dag decorator
@dag(
    dag_id='walmart_seller.dag_get_all_report',
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 */2 * * *",  # chạy mỗi 2 tiếng, vào phút 0
    catchup=False,
    tags=['walmart_seller']
)
def walmart_seller_report_dag():
    @task
    def sync_data_to_sharepoint_task():
        walmart_seller_dag = WalmartSellerSourceDAG()
        walmart_seller_dag.sync_data_to_sharepoint(BUYBOX_CONFIG)
    
    # Get the base DAG tasks
    walmart_seller_dag = WalmartSellerSourceDAG()
    refresh_credentials_task, create_all_reports_task, monitor_and_save_reports_task = walmart_seller_dag.create_dag_tasks()
    
    # Execute the tasks
    credentials_refreshed = refresh_credentials_task()
    reports_data = create_all_reports_task()
    final_results = monitor_and_save_reports_task(reports_data)
    
    # Set dependencies
    credentials_refreshed >> reports_data >> final_results >> sync_data_to_sharepoint_task()
    
    return {
        'credentials_refreshed': credentials_refreshed,
        'reports_created': reports_data,
        'reports_completed': final_results
    }

# Create the DAG instance
dag_instance = walmart_seller_report_dag() 