from dags.common.base_source_dag import BaseSourceDAG
from services.request_client import SourceConfig, WayfairApiType, create_source_client, SourceType

from services.notification_handler import send_failure_notification
import json
import os
import logging
from config.wayfair_dag_configs import PRODUCT_DETAIL_PAGE, PRODUCT_INFO, PRODUCT_DIMENSIONS, PRODUCT_SPECIFICATION, PRODUCT_LIST
from datetime import datetime
from services.wayfair_service import WayfairService
class WayfairGetProductDetail(BaseSourceDAG):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = f'data/wayfair/{datetime.now().year}/{datetime.now().month}/{datetime.now().day}/product_detail/product_detail_page'
    

    def get_items_to_process(self, mode):
        
        os.makedirs(self.path, exist_ok=True)
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
            df = pd.DataFrame(values[1:], columns=headers)
            df.rename(columns={'Competitor Relevant Product ID on Platform': 'sku', 'Competitor Relevant Product Link': 'url'}, inplace=True)
            df = df.drop_duplicates(subset=['sku'])
            df = df[['sku', 'url']]
            all_results = df.to_dict(orient='records')
            all_results = [
                {
                    'sku': item['sku'],
                    'method': 'POST',
                    'url': 'https://www.wayfair.com/a/product/get_joined_product',
                    'params': {
                        'sku': item['sku']
                    }
                }
                for item in all_results
            ]
            return all_results
        
        def get_processed_items():
            processed_items = []
            for file in os.listdir(self.path):
                if file.endswith('.json'):
                    with open(os.path.join(self.path, file), 'r') as f:
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
        sku = metadata['sku']
        return f"{sku}.json"


class WayfairGetProductDimensions(BaseSourceDAG):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = f'data/wayfair/{datetime.now().year}/{datetime.now().month}/{datetime.now().day}/product_detail/product_dimensions'


    def get_items_to_process(self, mode):
        path = self.path
        all_items = WayfairService().get_product_variations(f'data/wayfair/{datetime.now().year}/{datetime.now().month}/{datetime.now().day}/product_detail/product_detail_page', has_variations=False)
        
        if mode == 'all':
            return [
                {
                    'sku': sku,
                    'url': PRODUCT_DIMENSIONS.api_url,
                    'method': 'POST',
                    'payload': {
                        'operationName': 'Queries_Product_WeightsAndDimensions__',
                        'variables': {
                            'sku': sku,
                        },
                        'extensions': {
                            'persistedQuery': {
                                'version': 1,
                                'sha256Hash': '8af395606197e405ce5734546f12159b28433db85d5faf0ae1a600afc92631be',
                            },
                        },
                    }
                }
                for sku in all_items
            ]
        elif mode == 'failed':
            success_items = WayfairService().get_success_product_variations(path, has_variations=False)
            failed_items = WayfairService().get_failed_product_variations(all_items, success_items, has_variations=False)
            return [
                {
                    'sku': sku,
                    'url': PRODUCT_DIMENSIONS.api_url,
                    'method': 'POST',
                    'payload': {
                        'operationName': 'Queries_Product_WeightsAndDimensions__',
                        'variables': {
                            'sku': sku,
                        },
                        'extensions': {
                            'persistedQuery': {
                                'version': 1,
                                'sha256Hash': '8af395606197e405ce5734546f12159b28433db85d5faf0ae1a600afc92631be',
                            },
                        },
                    }
                }
                for sku in failed_items
            ]
        else:
            raise ValueError(f"Unsupported mode: {mode}")
    
    def build_file_name(self, metadata):
        sku = metadata.get('sku')
        return f"{sku}.json"


class WayfairGetProductInfo(BaseSourceDAG):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = f'data/wayfair/{datetime.now().year}/{datetime.now().month}/{datetime.now().day}/product_detail/product_info'
        os.makedirs(self.path, exist_ok=True)


    def get_items_to_process(self, mode):
        path = self.path
        all_items = WayfairService().get_product_variations(f'data/wayfair/{datetime.now().year}/{datetime.now().month}/{datetime.now().day}/product_detail/product_detail_page')
        
        if mode == 'all':
            return [
                {
                    'sku': item['sku'],
                    'selected_options': item['options'],
                    'url': PRODUCT_INFO.api_url,
                    'method': 'POST',
                    'payload': {
                        'variables': {
                            'sku': item['sku'],
                            'selectedOptionIds': item['options'],
                            'energyLabelContext': 'PDPCAROUSEL',
                        },
                    },
                    'params': None
                }
                for item in all_items
            ]
        elif mode == 'failed':
            success_items = WayfairService().get_success_product_variations(path)
            failed_items = WayfairService().get_failed_product_variations(all_items, success_items)
            return [
                {
                    'sku': item['sku'],
                    'selected_options': item['options'],
                    'url': PRODUCT_INFO.api_url,
                    'method': 'POST',
                    'payload': {
                        'variables': {
                            'sku': item['sku'],
                            'selectedOptionIds': item['options'],
                            'energyLabelContext': 'PDPCAROUSEL',
                        },
                    },
                    'params': None
                }
                for item in failed_items
            ]
        else:
            raise ValueError(f"Unsupported mode: {mode}")
    
    def build_file_name(self, metadata):
        sku = metadata.get('sku')
        selected_options = metadata.get('selected_options', [])
        
        if not sku and 'payload' in metadata:
            sku = metadata['payload'].get('variables', {}).get('sku')
            selected_options = metadata.get('payload', {}).get('variables', {}).get('selectedOptionIds', [])
        
        if not sku:
            raise ValueError("SKU not found in metadata")
            
        if selected_options:
            return f"{sku}_{'_'.join(selected_options)}.json"
        else:
            return f"{sku}.json"


class WayfairGetProductSpecification(BaseSourceDAG):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = f'data/wayfair/{datetime.now().year}/{datetime.now().month}/{datetime.now().day}/product_detail/product_specification'


    def get_items_to_process(self, mode):
        path = self.path
        all_items = WayfairService().get_product_variations(f'data/wayfair/{datetime.now().year}/{datetime.now().month}/{datetime.now().day}/product_detail/product_detail_page', has_variations=False)
        
        if mode == 'all':
            return [
                {
                    'sku': sku,
                    'url': PRODUCT_SPECIFICATION.api_url,
                    'method': 'POST',
                    'payload': {
                        'sku': sku,
                        'extensions': {
                            'persistedQuery': {
                                'version': 1,
                                'sha256Hash': '731f41b9572fefb3f47cddc6ab143d198903c8475f753210b4fb044c89d912a4',
                            },
                        },
                    }
                }
                for sku in all_items
            ]
        elif mode == 'failed':
            success_items = WayfairService().get_success_product_variations(path, has_variations=False)
            failed_items = WayfairService().get_failed_product_variations(all_items, success_items, has_variations=False)
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
        sku = metadata.get('sku')
        return f"{sku}.json"



class WayfairGetProductList(BaseSourceDAG):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = f'data/wayfair/{datetime.now().year}/{datetime.now().month}/{datetime.now().day}/product_list'
        os.makedirs(self.path, exist_ok=True)

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
            return [
                {
                    'url': item['url'],
                    'method': 'GET',
                    'keyword': item.get('keyword', ''),
                    'page_number': item.get('page_number', 1),
                    'params': {
                        'keyword': item.get('keyword', ''),
                        'page': item.get('page_number', 1)
                    }
                }
                for item in all_data
            ]
        elif mode == 'all':
            # For 'all' mode, you might want to process all items from the Excel file
            # This is similar to 'failed' mode but without filtering
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
            return [
                {
                    'url': item['url'],
                    'method': 'GET',
                    'keyword': item.get('keyword', ''),
                    'page_number': item.get('page_number', 1),
                    'params': {
                        'keyword': item.get('keyword', ''),
                        'page': item.get('page_number', 1)
                    }
                }
                for item in all_data
            ]
        else:
            raise ValueError(f"Unsupported mode: {mode}")

    def build_file_name(self, metadata):
        # Try to get parameters from metadata directly first (new approach)
        keyword = metadata.get('keyword', 'unknown')
        page_number = metadata.get('page_number', 1)
        return f'{keyword}_{page_number}.json'
# Define the DAG types and their configurations
dag_types = [
    {   'dag_id': 'wayfair.dag_get_product_detail',
     
        'name': 'product_detail',
        'config': PRODUCT_DETAIL_PAGE,
        'class': WayfairGetProductDetail

    },
    {
        'dag_id': 'wayfair.dag_get_product_info',
        'name': 'product_info',
        'config': PRODUCT_INFO,
        'class': WayfairGetProductInfo
 
    },
    {
        'dag_id': 'wayfair.dag_get_product_dimensions',
        'name': 'product_dimensions',
        'config': PRODUCT_DIMENSIONS,
        'class': WayfairGetProductDimensions

    },
    {
        'dag_id': 'wayfair.dag_get_product_specification',
        'name': 'product_specification',
        'config': PRODUCT_SPECIFICATION,
        'class': WayfairGetProductSpecification

    },
    {
        'dag_id': 'wayfair.dag_get_product_list',
        'name': 'product_list',
        'config': PRODUCT_LIST,
        'class': WayfairGetProductList

    }
    
]
from airflow.decorators import dag,task
from airflow.utils.dates import days_ago


# Create DAGs dynamically using a for loop
# Create DAGs for each table code
def make_wayfair_dag(dag_cfg: dict):
    @dag(
        dag_id=dag_cfg["dag_id"],
        tags=["wayfair", dag_cfg["name"]],
        schedule_interval=None,
        start_date=days_ago(1),
        catchup=False,
        on_failure_callback=send_failure_notification
    )
    def _dag():
        source_client = create_source_client(SourceType.WAYFAIR, dag_cfg['config'])
        dag_instance = dag_cfg['class'](dag_cfg['config'], source_client)
        return dag_instance.create_dag_tasks()
    return _dag()

for dag_cfg in dag_types:
    globals()[f'wayfair_dag_{dag_cfg["name"].replace(".", "_")}'] = make_wayfair_dag(dag_cfg)
        
        