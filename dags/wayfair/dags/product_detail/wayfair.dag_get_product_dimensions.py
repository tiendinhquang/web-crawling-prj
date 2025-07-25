from dags.common.base_source_dag import BaseSourceDAG
from services.request_client import SourceConfig, WayfairApiType, create_source_client, SourceType
from utils.common.metadata_manager import get_latest_folder


from dags.notification_handler import send_failure_notification
import json
import os
import logging
from config.wayfair_dag_configs import PRODUCT_DIMENSIONS
class WayfairGetProductInfo(BaseSourceDAG):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_items_to_process(self, mode):
        import dags.wayfair.common_etl as etl 
        path = get_latest_folder('data/wayfair') + "/" + PRODUCT_DIMENSIONS.base_path
        all_items = etl.get_product_variations(get_latest_folder('data/wayfair') + '/product_detail/product_detail_page', has_variations=False)
        if mode == 'all':
            return [
                {
                    'sku': sku,
                    'url': PRODUCT_DIMENSIONS.api_url,
                    'method': 'POST',
              
                }
                for sku in all_items
            ]
        elif mode == 'failed':
            success_items = etl.get_success_product_variations(path, has_variations=False)
            failed_items = etl.get_failed_product_variations(all_items, success_items, has_variations=False)
            return [
                {
                    'sku': sku,
                    'url': PRODUCT_DIMENSIONS.api_url,
                    'method': 'POST',
              
                }
                for sku in failed_items
            ]
        else:
            raise ValueError(f"Unsupported mode: {mode}")
    def build_file_name(self, metadata):
        # Try to get sku from metadata directly first (new approach)
        sku = metadata.get('sku')
        selected_options = metadata.get('selected_options', [])
        
        # Fallback to payload structure if not found directly (backward compatibility)
        if not sku and 'payload' in metadata:
            sku = metadata['payload'].get('variables', {}).get('sku')
            selected_options = metadata.get('payload', {}).get('variables', {}).get('selectedOptions', [])
        
        if not sku:
            raise ValueError("SKU not found in metadata")
            
        if selected_options:
            return f'{sku}_{'_'.join(selected_options)}.json'
        else:
            return f'{sku}.json'
            
            

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

@dag(
    dag_id='wayfair.dag_get_product_dimensions',
    description='Get Wayfair product dimensions by SKU',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    on_failure_callback=send_failure_notification,
)

def create_dag_product_info_instance():
    logging.info(f"🔄 Creating DAG instance with config: {PRODUCT_DIMENSIONS}")
    source_client = create_source_client(SourceType.WAYFAIR, PRODUCT_DIMENSIONS)
    dag_instance = WayfairGetProductInfo(PRODUCT_DIMENSIONS, source_client)
    return dag_instance.create_dag_tasks()
create_dag_product_info_instance()