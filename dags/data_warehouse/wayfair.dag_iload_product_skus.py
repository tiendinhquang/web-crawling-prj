from dags.data_warehouse.base_wayfair_db_load import BaseWayfairDBLoad
from utils.common.file_loader import read_csv
from airflow.decorators import dag, task
import pandas as pd
import logging
from utils.common.metadata_manager import get_latest_folder
from dags.notification_handler import send_success_notification, send_failure_notification
import json
from utils.common.file_loader import read_csv
import os

class WayfairDBLoaderProductSkus(BaseWayfairDBLoad):
    def __init__(self):
        super().__init__('wayfair.product_skus')


    def process_info_data(self, info_path):
        """
        Process info data from Wayfair product API response to extract key product details.
        
        Args:
            info_path (str): Path to the info data JSON file
            
        Returns:
            dict: Processed product information including:
                - Basic details (id, sku, title, url, etc.)
                - Images (img_id, img_url) 
                - Pricing (price, list_price)
                - Reviews (average_rating, review_count)
                - Variations (variation_name, variations)
                - Other metadata (manufacturer, stock_status)
                
        Note: The img_url is not the exact url from Wayfair, so it may be incorrect in some cases
        """
        def _has_server_error(data):
            """Check if response contains server error"""
            return (isinstance(data.get('errors'), list) and 
                    any(e.get('message') == 'Internal server error' for e in data['errors']))

        def _get_image_id(item):
            """Extract image ID from item data"""
            return str(item.get('leadImage', {}).get('id'))

        def _build_image_url(img_id):
            """Build full image URL from image ID"""
            return f"https://assets.wfcdn.com/im/18357803/resize-h300-w300%5Ecompr-r85/{img_id[:4]}/{img_id}/.jpg"

        def _build_product_url(item, variations):
            """Build product URL with variations"""
            return f"{item.get('url')}?piid={','.join(variations)}"

        def _get_price(item):
            """Extract current price from pricing data"""
            prices = item.get('pricing', {}) or {}
            prices = prices.get('priceBlockElements', [])
            for price_item in prices:
                if price_item.get('__typename') == 'SFPricing_CustomerPrice':
                    display = price_item.get('display', {})
                    return display.get('value') or display.get('min', {}).get('value')
            return None

        def _get_list_price(item):
            """Extract list price from pricing data"""
            prices = item.get('pricing', {}) or {}
            prices = prices.get('priceBlockElements', [])
            for price_item in prices:
                if price_item.get('__typename') == 'SFPricing_ListPrice':
                    return price_item['display']['value']
            return None

        def _process_variations(item, variations):
            """Process and validate product variations"""
            # Get all valid variation IDs
            option_categories = item.get('options', {}).get('optionCategories', [])
            all_variations = [str(option.get('id')) 
                            for category in option_categories 
                            for option in category.get('options', [])]
            
            # Replace invalid variations with defaults
            for i, var in enumerate(variations):
                if var not in all_variations:
                    variations[i] = str(item.get('defaultOptions', [])[0])
            
            # Build variation name mapping
            variation_name = {}
            for category in option_categories:
                category_name = category.get('categoryName', '').lower()
                for option in category.get('options', []):
                    if str(option.get('id')) in variations:
                        variation_name[category_name] = option.get('name')
                        
            return variation_name
        # Read and validate input data
        with open(info_path, 'r', encoding='utf-8') as f:
            info_data = json.load(f)
            
        if _has_server_error(info_data):
            logging.error(f"Error in {info_path}")
            return {}

        # Extract product data
        product_data = info_data.get('data', {}).get('product', {})
        cross_sell = product_data.get('crossSell', {})
        id = info_path.split('/')[-1].replace('.json', '')
        
   
            
        # Parse SKU and variations
        sku, *variations = id.split('_')
        
        # Find matching item in cross-sell data
        raw_data = cross_sell.get('items', [])
        matched_item = next((item for item in raw_data if item.get('sku') == sku), None)
        
        if not matched_item:
            return {}

        # Process variations
        variation_name = _process_variations(matched_item, variations)
        
        # Build result dictionary with all product info
        results = {
            "id": id,
            "sku": sku,
            "url": _build_product_url(matched_item, variations),
            'img_id': _get_image_id(matched_item),
            "img_url": _build_image_url(_get_image_id(matched_item)),
            "variation_name": json.dumps(variation_name),
            "variations": variations,
            "title": matched_item.get('productName'),
            "manufacturer": matched_item.get('manufacturer', {}).get('name'),
            "price": _get_price(matched_item),
            "list_price": _get_list_price(matched_item),
            "average_rating": matched_item.get('reviews', {}).get('reviewRating'),
            "review_count": matched_item.get('reviews', {}).get('reviewCount'),
            "stock_status": (matched_item.get('inventory') or {}).get('stockStatus')
        }
        
        return results
    

    def process_data(self):
        import os
        import dags.wayfair.common_etl as etl
        import csv
        from utils.common.sharepoint.sharepoint_manager import ExcelOnlineLoader
        excel_online_loader = ExcelOnlineLoader()
        site_id = 'atlasintl.sharepoint.com,ca126560-d5a9-4ea1-ace9-4361095e806f,1704d547-3d85-4235-9d34-ab57ec9572c4'
        drive_name = 'Documents'
        file_path = 'Web Crawling/Wayfair/Competitors/Competitors Master File.xlsx'
        sheet_name = 'WF Competitor List'
        range_address = 'F:G'

        data = excel_online_loader.get_used_range(site_id, drive_name, file_path, sheet_name, range_address)
        values = data['text']
        headers = values[0]
        df =  pd.DataFrame(values[1:], columns=headers)
        df.rename(columns={'Competitor Relevant Product ID on Platform': 'sku','Competitor Relevant Product Link': 'url'}, inplace=True)
        df = df.drop_duplicates(subset=['sku'])
        df = df[['sku', 'url']]
        
        all_data = df.to_dict(orient='records')
        results = []
        root = get_latest_folder(base_dir='data/wayfair')
        info_path = os.path.join(root, 'product_detail/product_info')
        for file in os.listdir(info_path) :
            if file.endswith('.json'):
                path = os.path.join(info_path, file)
                results.append(self.process_info_data(path))
        final_results = [item for item in results if item['sku'] in [data['sku'] for data in all_data]]
        df = pd.DataFrame(final_results)
        os.makedirs(f'{root}/output', exist_ok=True)
        df.to_csv(f'{root}/output/product_skus.csv', index=False, quoting=csv.QUOTE_ALL)
        return df
                
        
        

    def get_data(self):
        root = get_latest_folder(base_dir='data/wayfair')
        df = read_csv(f'{root}/output/product_skus.csv')
        df['ownership'] = 'competitor'
        df['from_src'] = 'wayfair'
        return df
    def load_data(self, df: pd.DataFrame):
        self.data_loader.iload_to_db('wayfair.product_skus', 'tmp_product_skus', df)

@dag(
    dag_id='wayfair.dag_iload_product_skus',
    schedule_interval=None,
    start_date=None,
    catchup=False,
    on_failure_callback=send_failure_notification
)
def wayfair_dag_iload_product_skus():
    dag_instance = WayfairDBLoaderProductSkus()
    return dag_instance.create_dag()

wayfairfair_dag_iload_product_skus_instance = wayfair_dag_iload_product_skus()