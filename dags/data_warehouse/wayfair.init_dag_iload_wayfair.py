import pandas as pd
import asyncio
import sys
import os
import json
from datetime import datetime
from dags.data_warehouse.base_db_load_dag import BaseDBLoadDAG
from airflow.decorators import dag
from services.notification_handler import send_failure_notification
from datetime import datetime
import logging
from zoneinfo import ZoneInfo

table_codes = ['wayfair.product_skus']

class DataProcessor:
    def __init__(self, table_config: dict):
        self.table_config = table_config
    def process_data(self):
        df = pd.DataFrame()
        return df
    def _has_server_error(self, data):
        """Check if response contains server error"""
        return (isinstance(data.get('errors'), list) and 
                any(e.get('message') == 'Internal server error' for e in data['errors']))

    def _get_image_id(self, item):
        """Extract image ID from item data"""
        return str(item.get('leadImage', {}).get('id'))

    def _build_image_url(self, img_id):
        """Build full image URL from image ID"""
        return f"https://assets.wfcdn.com/im/18357803/resize-h300-w300%5Ecompr-r85/{img_id[:4]}/{img_id}/.jpg"

    def _build_product_url(self, item, variations):
        """Build product URL with variations"""
        return f"{item.get('url')}?piid={','.join(variations)}"

    def _get_price(self, item):
        """Extract current price from pricing data"""
        prices = item.get('pricing', {}) or {}
        prices = prices.get('priceBlockElements', [])
        for price_item in prices:
            if price_item.get('__typename') == 'SFPricing_CustomerPrice':
                display = price_item.get('display', {})
                return display.get('value') or display.get('min', {}).get('value')
        return None

    def _get_list_price(self, item):
        """Extract list price from pricing data"""
        prices = item.get('pricing', {}) or {}
        prices = prices.get('priceBlockElements', [])
        for price_item in prices:
            if price_item.get('__typename') == 'SFPricing_ListPrice':
                return price_item['display']['value']
        return None

    def _process_variations(self, item, variations):
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

        # Read and validate input data
        with open(info_path, 'r', encoding='utf-8') as f:
            info_data = json.load(f)
            
        if self._has_server_error(info_data):
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
        variation_name = self._process_variations(matched_item, variations)
        
        # Build result dictionary with all product info
        results = {
            "id": id,
            "sku": sku,
            "url": self._build_product_url(matched_item, variations),
            'img_id': self._get_image_id(matched_item),
            "img_url": self._build_image_url(self._get_image_id(matched_item)),
            "variation_name": json.dumps(variation_name),
            "variations": variations,
            "title": matched_item.get('productName'),
            "manufacturer": matched_item.get('manufacturer', {}).get('name'),
            "price": self._get_price(matched_item),
            "list_price": self._get_list_price(matched_item),
            "average_rating": matched_item.get('reviews', {}).get('reviewRating'),
            "review_count": matched_item.get('reviews', {}).get('reviewCount'),
            "stock_status": (matched_item.get('inventory') or {}).get('stockStatus')
        }
        
        return results
    def process_all_info_data(self,process_date):
        import os
        import csv
        from utils.common.sharepoint.sharepoint_manager import ExcelOnlineLoader
        
        logging.info("Starting process_data for Wayfair product SKUs")
        
        try:
            excel_online_loader = ExcelOnlineLoader()
            site_id = 'atlasintl.sharepoint.com,ca126560-d5a9-4ea1-ace9-4361095e806f,1704d547-3d85-4235-9d34-ab57ec9572c4'
            drive_name = 'Documents'
            file_path = 'Web Crawling/Wayfair/Competitors/Competitors Master File.xlsx'
            sheet_name = 'WF Competitor List'
            range_address = 'F:G'

            logging.info("Loading data from SharePoint Excel file")
            data = excel_online_loader.get_used_range(site_id, drive_name, file_path, sheet_name, range_address)
            values = data['text']
            headers = values[0]
            df = pd.DataFrame(values[1:], columns=headers)
            df.rename(columns={'Competitor Relevant Product ID on Platform': 'sku','Competitor Relevant Product Link': 'url'}, inplace=True)
            df = df.drop_duplicates(subset=['sku'])
            df = df[['sku', 'url']]
            logging.info(f"Loaded {len(df)} competitor records from SharePoint")
            
            all_data = df.to_dict(orient='records')
            results = []
            info_path = f'data/wayfair/{process_date.year}/{process_date.month}/{process_date.day}/product_detail/product_info'
            
            logging.info(f"Processing product info files from: {info_path}")
            json_files = [f for f in os.listdir(info_path) if f.endswith('.json')]
            logging.info(f"Found {len(json_files)} JSON files to process")
            
            for file in json_files:
                path = os.path.join(info_path, file)
                result = self.process_info_data(path)
                if result:  # Only add non-empty results
                    results.append(result)
            
            logging.info(f"Processed {len(results)} product info records")
            
            final_results = [item for item in results if item['sku'] in [data['sku'] for data in all_data]]
            logging.info(f"Filtered to {len(final_results)} matching competitor products")

            # Convert final_results to DataFrame with all required columns
            
            if final_results:
                result_df = pd.DataFrame(final_results)
                result_df['ownership'] = 'competitor'
                logging.info(f"Created DataFrame with columns: {list(result_df.columns)}")
                return result_df
            else:
                logging.warning("No matching results found, returning empty DataFrame")
                return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error in process_data: {e}")
            raise


                    
class CriteoDBLoader(BaseDBLoadDAG):
    def __init__(self, table_code: str):
        super().__init__(table_code)
    
        
    def process_data(self):
        data_processor = DataProcessor(self.table_config)
        if self.table_code == 'wayfair.product_skus':
            df = data_processor.process_all_info_data(datetime.now())
        
        return df
    
    def load_data(self, df: pd.DataFrame):
        logging.info(f"Loading data with shape {df.shape}")
        self.data_loader.iload_to_db(self.table_code, f'tmp_{self.table_config["des_table"]}', df)


# Create DAGs for each table code
def make_wayfair_dag(table_code: str):
    @dag(
        dag_id=f'wayfair.dag_iload_{table_code.split(".")[1]}',
        tags=["wayfair", 'data warehouse', 'iload'],
        schedule_interval=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        on_failure_callback=send_failure_notification
    )
    def _dag():
        dag_instance = CriteoDBLoader(table_code)
        return dag_instance.create_dag()
    return _dag()

for code in table_codes:
    globals()[f'wayfair_dag_{code.replace(".", "_")}'] = make_wayfair_dag(code)
