import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from airflow.decorators import dag, task
from dags.common.base_init_reports_dag import BaseReportsDAG
from services.gg_merchants_service import GGMerchantsService
from datetime import datetime, timedelta
import asyncio
import logging

sku_visibility = {
    'date_range_days': 30,
    'report_type': 'sku_visibility',
    'base_path': 'data/gg_merchants',
    
}

download_config = {
    'sku_visibility': {
        'prefix': 'sku_visibility',
        'base_path': 'data/gg_merchants/sku_visibility',
        'bucket_name': 'atlasusa-ap-southeast-1-bi-auto-bucket',
        "bucket_path": "prod/crawler/bronze/gg_merchants/sku_visibility/",
    }
}
class GGMerchantsSourceDAG(BaseReportsDAG):
    """GG Merchants-specific implementation of the base source DAG"""
    
    def __init__(self):
        super().__init__(
            dag_id='gg_merchants.dag_get_all_report',
        )
        self.service = None
    
    def get_service(self):
        """Return the GG Merchants service instance"""
        if not self.service:
            self.service = GGMerchantsService()
        return self.service
    
    def get_report_configs(self, context=None):
        """Return the list of GG Merchants report configurations"""
        dag_run = context.get("dag_run") if context else None
        dag_run_conf = dag_run.conf if dag_run else {}
        if dag_run_conf and isinstance(dag_run_conf, dict) and 'reports' in dag_run_conf:
            reports = dag_run_conf['reports']
            if isinstance(reports, list):
                selected_configs = []
                for report_type in reports:
                    if report_type == 'sku_visibility':
                        selected_configs.append(sku_visibility)
                return selected_configs
            else:
                logging.warning(f"Invalid reports configuration: {reports}")
                return [sku_visibility]
        else:
            logging.warning("No reports configuration found in dag_run.conf")
            return [sku_visibility]
    
    def refresh_credentials(self):
        """Refresh GG Merchants cookies and update config"""
        service = self.get_service()
        asyncio.run(service.refresh_cookies_and_update_config())
    
    async def create_report(self, report_config):
        """Create a GG Merchants report and return the response"""
        service = self.get_service()
        end_date = datetime.now()
        start_date = end_date - timedelta(days=report_config['date_range_days'])
        # start_date = datetime(2025, 1, 1)
        
        return await service.create_sku_visibility_report(
            start_date=start_date,
            end_date=end_date
        )
    
    async def get_report_status(self, report_response, report_config):
        """Get the status of a GG Merchants report"""
        service = self.get_service()
        
        # For GG Merchants, we need to get all reports and filter by request time
        status = await service.get_report_status(report_response)
        
        # Return a status object that matches what is_report_ready expects
        return {'status': status}
    
    def is_report_ready(self, status_response):
        """Check if a GG Merchants report is ready"""
        status = status_response.get('status')
        logging.info(f"Checking if report is ready. Status: {status}, Full response: {status_response}")
        return status == 'DONE'
    
    async def save_report(self, report_id, report_config, status_response):
        """Save a completed GG Merchants report"""
        service = self.get_service()
        
        logging.info(f"Saving report {report_config['report_type']} with status: {status_response}")
        
        try:
            # Download the report data
            report_data = await service.get_report_data(report_id)
            bucket_path = download_config[report_config['report_type']]['bucket_path']
            prefix = download_config[report_config['report_type']]['prefix']
            s3_key = f"{bucket_path}/{prefix}_{datetime.now().strftime('%Y%m%d')}.csv"
            await service.save_report_to_s3(
                report_type=report_config['report_type'],
                report_data=report_data,
                base_path=download_config[report_config['report_type']]['base_path'],
                s3_key=s3_key,
                process_date=datetime.now(),
                bucket_name=download_config[report_config['report_type']]['bucket_name']
            )
        except Exception as e:
            logging.error(f"Error saving report {report_config['report_type']}: {str(e)}")
            raise
    
    def get_report_identifier(self, report_config):
        """Get a unique identifier for the GG Merchants report"""
        return report_config['report_type']

# Create the DAG using the @dag decorator
@dag(
    dag_id='gg_merchants.dag_get_all_report',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['gg_merchants']
)
def gg_merchants_report_dag():
    # Get the base DAG tasks
    gg_merchants_dag = GGMerchantsSourceDAG()
    refresh_credentials_task, create_all_reports_task, monitor_and_save_reports_task = gg_merchants_dag.create_dag_tasks()
    
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
dag_instance = gg_merchants_report_dag() 