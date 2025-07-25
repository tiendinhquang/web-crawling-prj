from services.request_client import SourceConfig, WayfairApiType, create_source_client, SourceType
from utils.common.metadata_manager import get_latest_folder
from dags.common.base_source_dag import BaseSourceDAG

from dags.notification_handler import send_failure_notification
import json
import os
import logging
from config.wayfair_dag_configs import PRODUCT_LIST
import urllib.parse
class WayfairGetProductList(BaseSourceDAG):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    def get_items_to_process(self, mode):
        if mode == 'failed':
            from utils.common.sharepoint.sharepoint_manager import ExcelOnlineLoader
            import pandas as pd 
            excel_online_loader = ExcelOnlineLoader()
            site_id = 'atlasintl.sharepoint.com,ca126560-d5a9-4ea1-ace9-4361095e806f,1704d547-3d85-4235-9d34-ab57ec9572c4'
            file_path = 'Web Crawling/Wayfair/Wayfair Product List/Wayfair Product Crawling.xlsx'
            drive_name = 'Documents'
            data = excel_online_loader.get_used_range(site_id,drive_name, file_path, 'Wayfair Product Crawling', 'A:C')
            values = data['text']
            headers = values[0]
            df =  pd.DataFrame(values[1:], columns=headers)
            all_data =  df.to_dict(orient='records')
            return [{
                  'url': item['url'],
                  'method': 'GET',
                  'keyword': item.get('keyword', ''),
                  'page_number': item.get('page_number', 1),
                  
            }
            for item in all_data]
    def build_file_name(self, metadata):
        # Try to get parameters from metadata directly first (new approach)
        keyword = metadata.get('keyword', 'unknown')
        page_number = metadata.get('page_number', 1)
        return f'{keyword}_{page_number}.json'


from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

@dag(
    dag_id='wayfair.dag_get_product_list',
    description='Get Wayfair product list by keyword',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    # on_failure_callback=send_failure_notification
)
def wayfair_get_product_list_dag():
    source_client = create_source_client(SourceType.WAYFAIR, PRODUCT_LIST)
    dag_instance = WayfairGetProductList(PRODUCT_LIST, source_client)
    return dag_instance.create_dag_tasks()

wayfair_get_product_list_dag()