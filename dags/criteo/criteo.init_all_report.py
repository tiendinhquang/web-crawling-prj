from dags.common.base_source_dag import BaseSourceDAG
from services.request_client import SourceConfig, create_source_client, SourceType, CriteoApiType
from utils.common.metadata_manager import get_latest_folder
from services.criteo_service import CriteoService
from services.notification_handler import send_failure_notification
import json
import os
import logging  
from config.criteo_dag_configs import CRITEO_CAPOUT_CONFIG, SEARCH_TERM_CONFIG, PLACEMENT_CONFIG, BID_MULTIPLIER_CONFIG, CAMPAIGN_CONFIG, LINE_ITEM_CONFIG

import urllib.parse
from datetime import datetime, timedelta

# Configuration for different API types to eliminate repetition
API_TYPE_CONFIGS = {
    CriteoApiType.BID.value: {}
}

class ParamsBuilder:
    def __init__(self, api_type: CriteoApiType):
        self.api_type = api_type
        self.config = API_TYPE_CONFIGS[api_type]
        
    def build_bid_params(self, line_item_id, **kwargs):
        """Build parameters for BID API type (no parameters needed)"""
        return {}
        

    def build_request_params(self, campaign_id, period=None, line_item_id=None, **kwargs):
        return self.build_bid_params(line_item_id, **kwargs)


class FileNameBuilder:
    def __init__(self, source_config):
        self.source_config = source_config
        
    def build_file_name(self, metadata):
        """Build file name based on API type and metadata"""
        campaign_id = metadata.get('campaign_id', '')
        line_item_id = metadata.get('line_item_id', '')
        if self.source_config.api_type == CriteoApiType.BID.value:
            return f"{line_item_id}_{self.source_config.base_path}.json"
        else:
            return f"{campaign_id}_{self.source_config.base_path}.json"


class CriteoItemsBuilder:
    def __init__(self, source_config):
        self.source_config = source_config
        self.params_builder = ParamsBuilder(self.source_config.api_type)
        self.config = API_TYPE_CONFIGS[source_config.api_type]


    def _process_all_mode(self, line_item_ids):
        """Process items for 'all' mode based on API type"""

        if self.source_config.api_type == CriteoApiType.BID.value:
            return self.process_by_line_item_ids(line_item_ids)


    def _process_failed_mode(self, line_item_ids):
        """Process items for 'failed' mode based on API type"""
        if self.source_config.api_type == CriteoApiType.BID.value:
            processed_line_item_ids = CriteoService().get_processed_line_item_ids(
                line_item_ids, datetime.now(), self.source_config.base_path
            )
            all_items = self.process_by_line_item_ids(line_item_ids)
            return [item for item in all_items if item['line_item_id'] not in processed_line_item_ids]
        else:
            return []

    def get_items_to_process(self, mode):
        """Get items to process based on mode"""
        if mode not in ['all', 'failed']:
            raise ValueError(f"Unsupported mode: {mode}. Supported modes: 'all', 'failed'")
            
        campaign_ids = CriteoService().get_campaign_ids()
        line_item_ids = CriteoService().get_line_item_ids()
        
        if mode == 'all':
            return self._process_all_mode(line_item_ids)
        elif mode == 'failed':
            return self._process_failed_mode(line_item_ids)

    def process_by_campaign_ids(self, campaign_ids, period):
        """Process items by campaign IDs (for CAPOUT)"""
        return [
            {
                'campaign_id': id,
                'period': period,
                'url': self.source_config.api_url,
                'method': 'GET',
                'payload': None,
                'params': self.params_builder.build_request_params(id, period)
            }
            for id in campaign_ids
        ]
    def process_by_line_item_ids(self, line_item_ids):
        """Process items by line item IDs (for BID_MULTIPLIER)"""
    
        return [
            {
                'line_item_id': item['id'],
                'url': self.source_config.api_url if self.source_config.api_url else f'https://retailmedia.criteo.com/gateway/v2/auction-ads/line-items/{item["id"]}/sponsored-products',
                'method': 'GET',
                'payload': None,
                'params': self.params_builder.build_request_params(id, None)  # Pass line_item_id as first param, None as period
            }
            for item in line_item_ids
        ]

class CriteoGetReportByDateRange(BaseSourceDAG):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.criteo_service = CriteoService()
        self.params_builder = ParamsBuilder(self.source_config.api_type)

    def get_items_to_process(self, start_date, end_date):
        period = f'{start_date}_{end_date}'
        return [
            {
                'period': period,
                'url': self.source_config.api_url,
                'method': 'GET',
                'payload': None,
                'params': self.params_builder.build_request_params(None, period)  # Pass None as campaign_id, period as second param
            }
        ]
        
    def get_processed_campaign_ids(self, period, campaign_ids, process_date, base_path):
        return CriteoService().get_processed_campaign_ids(period, campaign_ids, process_date, base_path)
        
    def get_processed_campaign_ids_by_date_range(self, process_date, start_date, end_date, base_path, campaign_ids):
        return CriteoService().get_processed_campaign_ids_by_date_range(process_date, start_date, end_date, base_path, campaign_ids)
    
    def get_processed_line_item_ids_by_date_range(self, process_date, start_date, end_date, base_path, line_item_ids, period):
        return CriteoService().get_processed_line_item_ids_by_date_range(process_date, start_date, end_date, base_path, line_item_ids, period)

class CriteoGetReport(BaseSourceDAG):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.criteo_service = CriteoService()
        self.params_builder = ParamsBuilder(self.source_config.api_type)
        self.items_builder = CriteoItemsBuilder(self.source_config)
        self.file_name_builder = FileNameBuilder(self.source_config)
        
    async def refresh_token(self):
        """Refresh Criteo bearer token and update header configuration"""
        try:
            logging.info("🔄 Starting Criteo token refresh...")
            success = await self.criteo_service.refresh_token_and_update_headers()
            
            if success:
                logging.info("✅ Criteo token refresh completed successfully")
            else:
                logging.error("❌ Criteo token refresh failed")
                
            return success
            
        except Exception as e:
            logging.error(f"❌ Error during Criteo token refresh: {e}")
            return False

    def get_items_to_process(self, mode):
        """Get items to process based on mode"""
        if mode not in ['all', 'failed']:
            raise ValueError(f"Unsupported mode: {mode}. Supported modes: 'all', 'failed'")
            
        return self.items_builder.get_items_to_process(mode)
        
    def build_file_name(self, metadata):
        return self.file_name_builder.build_file_name(metadata)

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

def create_dag_report_instance(config: SourceConfig):
    """Create DAG instance with the given configuration"""
    logging.info(f"🔄 Creating DAG instance with config: {config}")
    source_client = create_source_client(SourceType.CRITEO, config)
    dag_instance = CriteoGetReport(config, source_client)
    
    @task
    def refresh_criteo_token():
        """Refresh Criteo bearer token before processing"""
        import asyncio
        try:
            # Run the async function in a new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(dag_instance.refresh_token())
            loop.close()
            return result
        except Exception as e:
            logging.error(f"Error refreshing token: {e}")
            return False
    
    token_refresh = refresh_criteo_token()
    choose, all_vars, failed_vars, run_all, run_failed = dag_instance.create_dag_tasks()
    
    # Set token refresh as upstream dependency
    choose.set_upstream(token_refresh)
    
    return choose, all_vars, failed_vars, run_all, run_failed

# Base DAG configuration to eliminate repetition
BASE_DAG_CONFIG = {
    'schedule_interval': None,
    'start_date': days_ago(1),
    'catchup': False,
    'max_active_runs': 1,
    'on_failure_callback': send_failure_notification,
}

# DAG configurations
DAG_CONFIGS = [

    {
        'dag_id': 'criteo.dag_get_bid_multiplier_report',
        'tags': ["criteo", "bid-multiplier"],
        'description': 'Get Criteo bid multiplier report',
        'config': BID_MULTIPLIER_CONFIG
    }

]

# Generate DAGs dynamically
for dag_cfg in DAG_CONFIGS:
    @dag(
        dag_id=dag_cfg['dag_id'],
        tags=dag_cfg['tags'],
        description=dag_cfg['description'],
        **BASE_DAG_CONFIG
    )
    def generated_dag():
        return create_dag_report_instance(dag_cfg['config'])
    
    globals()[dag_cfg['dag_id']] = generated_dag()



