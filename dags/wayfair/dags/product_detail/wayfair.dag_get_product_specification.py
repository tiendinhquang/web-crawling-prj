from dags.common.base_source_dag import BaseSourceDAG
from services.request_client import SourceConfig, WayfairApiType, create_source_client, SourceType
from utils.common.metadata_manager import get_latest_folder


from services.notification_handler import send_failure_notification
import json
import os
import logging
from config.wayfair_dag_configs import PRODUCT_SPECIFICATION
class WayfairGetProductInfo(BaseSourceDAG):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_items_to_process(self, mode):
        import dags.wayfair.common_etl as etl 
        path = get_latest_folder('data/wayfair') + "/" + PRODUCT_SPECIFICATION.base_path
        all_items = etl.get_product_variations(get_latest_folder('data/wayfair') + '/product_detail/product_detail_page', has_variations=False)
        if mode == 'all':
            return [
                {
                    'sku': sku,
                    'url': PRODUCT_SPECIFICATION.api_url,
                    'method': 'POST',
                    'payload': {
                            'sku': sku,
                        },
                        'extensions': {
                            'persistedQuery': {
                                'version': 1,
                                'sha256Hash': '731f41b9572fefb3f47cddc6ab143d198903c8475f753210b4fb044c89d912a4',
                            },
                        },
                    }

              
                
                for sku in all_items
            ]
        elif mode == 'failed':
            success_items = etl.get_success_product_variations(path, has_variations=False)
            failed_items = etl.get_failed_product_variations(all_items, success_items, has_variations=False)
            return [
                {
                    'sku': sku,
                    'url': PRODUCT_SPECIFICATION.api_url,
                    'method': 'POST',
                    'payload': {
                        'operationName': 'specs',
                        'variables': {
                            'sku': sku,
                        },
                        'extensions': {
                            'persistedQuery': {
                                'version': 1,
                                'sha256Hash': '731f41b9572fefb3f47cddc6ab143d198903c8475f753210b4fb044c89d912a4',
                            },
                        },
                    }

              
                }
                for sku in failed_items
            ]
        else:
            raise ValueError(f"Unsupported mode: {mode}")
    def build_file_name(self, metadata):
        # Try to get sku from metadata directly first (new approach)
        sku = metadata.get('sku')
        return f"{sku}.json"
            
            

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

@dag(
    dag_id='wayfair.dag_get_product_specification',
    tags=["wayfair", "product_specification"],
    description='Get Wayfair product specification by SKU',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    on_failure_callback=send_failure_notification,
)

def create_dag_product_info_instance():
    logging.info(f"🔄 Creating DAG instance with config: {PRODUCT_SPECIFICATION}")
    source_client = create_source_client(SourceType.WAYFAIR, PRODUCT_SPECIFICATION)
    dag_instance = WayfairGetProductInfo(PRODUCT_SPECIFICATION, source_client)
    return dag_instance.create_dag_tasks()
create_dag_product_info_instance()