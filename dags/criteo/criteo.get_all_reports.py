import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from airflow.decorators import dag, task
from dags.common.base_init_reports_dag import BaseReportsDAG
from services.criteo_service import CriteoService
from datetime import datetime, timedelta
import asyncio
import logging

# Report configurations
line_items = {
    'date_range_days': 30,
    "dimensions": "campaign_name,date,line_item_name,adv_product_id,campaign_id,line_item_id",
    "metrics": "impressions,clicks,ctr,win_rate,total_spend,cpc,unique_visitors,frequency,assisted_units,assisted_sales,attributed_units,attributed_sales,roas,discarded_product_clicks,new_to_global_brand_attributed_sales",
    'report_type': 'line_item',
    'base_path': 'data/criteo',
}

search_term = {
    'date_range_days': 30,
    "dimensions": "campaign_name,date,raw_search_term,activity_search_term_type,activity_search_term_targeting,campaign_id",
    "metrics": "impressions,clicks,ctr,win_rate,total_spend,cpc,unique_visitors,frequency,assisted_units,assisted_sales,attributed_units,attributed_sales,roas,discarded_product_clicks,new_to_global_brand_attributed_sales",
    'report_type': 'search_term',
    'base_path': 'data/criteo',
}
placement = {
    'date_range_days': 30,
    'dimensions': 'campaign_name,date,line_item_name,page_type_name,campaign_id,line_item_id,page_type_id',
    "metrics": "impressions,clicks,ctr,win_rate,total_spend,cpc,unique_visitors,frequency,assisted_units,assisted_sales,attributed_units,attributed_sales,roas,discarded_product_clicks",
    'report_type': 'placement',
    'base_path': 'data/criteo',
}

capout = {
    'date_range_days': 7,
    "dimensions": "campaign_name,date,campaign_id",
    "metrics": "dayparting_scheduled,capout_hour,capout_missed_traffic,total_spend,capout_missed_spend,attributed_sales,capout_missed_sales,impressions,capout_missed_impressions,clicks,capout_missed_clicks,ctr,cpc,roas",
    'report_type': 'capout',
    'base_path': 'data/criteo',
}
campaign = {
    'date_range_days': 30,
    "dimensions": "campaign_name,date,campaign_id",
    "metrics": "impressions,clicks,ctr,win_rate,total_spend,cpc,unique_visitors,frequency,assisted_units,assisted_sales,attributed_units,attributed_sales,roas,discarded_product_clicks,new_to_global_brand_attributed_sales",
    'report_type': 'campaign',
    'base_path': 'data/criteo',
}

attributed_transaction = {
    'date_range_days': 7,
    'report_type': 'attributed_transaction',
    'base_path': 'data/criteo',
}
class CriteoSourceDAG(BaseReportsDAG):
    """Criteo-specific implementation of the base source DAG"""
    def __init__(self):
        super().__init__(
            dag_id='criteo.get_all_reports',
        )
        self.service = None
        
        
    def get_service(self):
        """Return the Criteo service instance"""
        if not self.service:
            self.service = CriteoService()
        return self.service

    def get_report_configs(self, context=None):
        """Return the list of Criteo report configurations based on dag_run.conf or default"""
        # Get configuration from dag_run.conf
        dag_run = context.get("dag_run") if context else None
        dag_run_conf = dag_run.conf if dag_run else {}
        
        if dag_run_conf and isinstance(dag_run_conf, dict) and 'reports' in dag_run_conf:
            reports = dag_run_conf['reports']
            
            # If reports is a list of report type names, map them to default configs
            if isinstance(reports, list):
                selected_configs = []
                for report_type in reports:
                    if report_type == 'line_items':
                        selected_configs.append(line_items)
                    elif report_type == 'search_term':
                        selected_configs.append(search_term)
                    elif report_type == 'placement':
                        selected_configs.append(placement)
                    elif report_type == 'capout':
                        selected_configs.append(capout)
                    elif report_type == 'campaign':
                        selected_configs.append(campaign)
                    elif report_type == 'attributed_transaction':
                        selected_configs.append(attributed_transaction)
                    else:
                        logging.warning(f"Unknown report type: {report_type}")
                
                if selected_configs:
                    return selected_configs
        
        # Return default configurations if no valid conf provided
        return 
    
    def refresh_credentials(self):
        """Refresh Criteo tokens and update headers"""
        service = self.get_service()
        asyncio.run(service.refresh_token_and_update_headers())
    
    async def create_report(self, report_config):
        """Create a Criteo report and return the report ID"""
        service = self.get_service()
        end_date = datetime.now()
        start_date = end_date - timedelta(days=report_config['date_range_days'])
        # start_date = datetime(2023, 8, 1)
        campaign_ids = None
        if report_config['report_type'] == 'attributed_transaction':
            campaign_ids = service.get_campaign_ids()
        return await service.create_report_by_date_range(
            start_date=start_date, 
            end_date=end_date, 
            report_type=report_config['report_type'],
            dimensions=report_config.get('dimensions', None), 
            metrics=report_config.get('metrics', None),
            campaign_ids=campaign_ids
        )
    
        
    async def get_report_status(self, report_id, report_config):
        """Get the status of a Criteo report"""
        service = self.get_service()
        end_date = datetime.now()
        start_date = end_date - timedelta(days=report_config['date_range_days'])
        campaign_ids = None
        if report_config['report_type'] == 'attributed_transaction':
            campaign_ids = service.get_campaign_ids()
        return await service.get_report_status(
            report_type=report_config['report_type'],
            report_id=report_id, 
            start_date=start_date, 
            end_date=end_date,
            dimensions=report_config.get('dimensions', None), 
            metrics=report_config.get('metrics', None),
            campaign_ids=campaign_ids
        )
        
    
    def is_report_ready(self, status_response):
        """Check if a Criteo report is ready"""
        if status_response.get('status') == 'ERROR':
            logging.error(f"Report {status_response.get('report_id')} failed with error: {status_response.get('error')}")
            return True
        return status_response.get('status') == 'DONE'
    
    async def save_report(self, report_id, report_config, status_response):
        """Save a completed Criteo report"""
        service = self.get_service()
        end_date = datetime.now()
        start_date = end_date - timedelta(days=report_config['date_range_days'])
        campaign_ids = None
        if report_config['report_type'] == 'attributed_transaction':
            campaign_ids = service.get_campaign_ids()
        # Generate file path
        date = datetime.now()
        business_date = date.strftime("%Y-%m-%d")
        year, month, day = date.year, date.month, date.day
        path = f"{report_config['base_path']}/{year}/{month}/{day}/{report_config['report_type']}/{report_config['report_type']}_{business_date}.csv"
        
        await service.save_report_data_to_local(
            report_type=report_config['report_type'], 
            report_id=report_id, 
            start_date=start_date, 
            end_date=end_date,
            dimensions=report_config.get('dimensions', None), 
            metrics=report_config.get('metrics', None),
            campaign_ids=campaign_ids,
            path=path
        )
        
        return path
    
    def get_report_identifier(self, report_config):
        """Get a unique identifier for the Criteo report"""
        return report_config['report_type']


# Create the DAG using the @dag decorator
@dag(
    dag_id='criteo.get_all_reports',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['criteo']
)
def criteo_report_dag():
    # Get the base DAG tasks
    criteo_dag = CriteoSourceDAG()
    refresh_credentials_task, create_all_reports_task, monitor_and_save_reports_task = criteo_dag.create_dag_tasks()
    
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
dag_instance = criteo_report_dag() 