from dags.common.base_source_dag import BaseSourceDAG
from services.request_client import SourceConfig, WayfairApiType, create_source_client, SourceType
from utils.common.metadata_manager import get_latest_folder


from dags.notification_handler import send_failure_notification
import json
import os
import logging
class WayfairGetProductDetail(BaseSourceDAG):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_items_to_process(self, mode):
        from datetime import datetime
        year, month, day  = datetime.now().year, datetime.now().month, datetime.now().day
        path = f'data/wayfair/{year}/{month}/{day}/product_detail/product_detail_page'
        os.makedirs(path, exist_ok=True)
        from utils.common.sharepoint.sharepoint_manager import ExcelOnlineLoader
        import pandas as pd 
        excel_online_loader = ExcelOnlineLoader()
        site_id = 'atlasintl.sharepoint.com,ca126560-d5a9-4ea1-ace9-4361095e806f,1704d547-3d85-4235-9d34-ab57ec9572c4'
        drive_name = 'Documents'
        file_path = 'Web Crawling/Wayfair/Competitors/Competitors Master File.xlsx'
        sheet_name = 'WF Competitor List'
        range_address = 'F:G'
        def get_all_items():
            data = excel_online_loader.get_used_range(site_id, drive_name, file_path, sheet_name, range_address)
            values = data['text']
            headers = values[0]
            df =  pd.DataFrame(values[1:], columns=headers)
            df.rename(columns={'Competitor Relevant Product ID on Platform': 'sku','Competitor Relevant Product Link': 'url'}, inplace=True)
            df = df.drop_duplicates(subset=['sku'])
            df = df[['sku', 'url']]
            all_results = df.to_dict(orient='records')
            all_results = [
                {
                    'sku': item['sku'],
                    'url': item['url'],
                    'method': 'POST',
                    'url': 'https://www.wayfair.com/a/product/get_joined_product'
                }
                for item in all_results
            ]
            return all_results
        
        def get_processed_items():
            path = get_latest_folder('data/wayfair') + '/product_detail/product_detail_page'
            os.makedirs(path, exist_ok=True)
            processed_items = []
            for file in os.listdir(path):
                if file.endswith('.json'):
                    with open(os.path.join(path, file), 'r') as f:
                        data = json.load(f)
                        if data.get('sku'):
                            processed_items.append(data.get('sku'))
                        else:
                            processed_items.append(file.split('_')[0].replace('.json', ''))
            return processed_items
        
        
        if mode == 'all':
            all_items = get_all_items()
            return all_items
        elif mode == 'failed':
            all_items = get_all_items()
            processed_items = get_processed_items()
            failed_items = [item for item in all_items if item['sku'] not in processed_items]
            return failed_items
        else:
            raise ValueError(f"Unsupported mode: {mode}")
    def build_file_name(self, metadata):
        sku = metadata['payload']['sku']
        selected_options = metadata.get('payload').get('selected_options', [])
        if selected_options:
            return f"{sku}_{'_'.join(selected_options)}.json"
        else:
            return f"{sku}.json"    
    
            
            
            
from config.wayfair_dag_configs import PRODUCT_DETAIL_PAGE
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

@dag(
    dag_id='wayfair.dag_get_product_detail',
    description='Get Wayfair product detail by SKU',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    on_failure_callback=send_failure_notification,
)

def create_dag_product_detail_instance():
    logging.info(f"🔄 Creating DAG instance with config: {PRODUCT_DETAIL_PAGE}")
    source_client = create_source_client(SourceType.WAYFAIR, PRODUCT_DETAIL_PAGE)
    dag_instance = WayfairGetProductDetail(PRODUCT_DETAIL_PAGE, source_client)
    return dag_instance.create_dag_tasks()
create_dag_product_detail_instance()