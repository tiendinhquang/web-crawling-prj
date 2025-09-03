from dags.common.base_reviews_dag import BaseReviewsDAG
from services.request_client import SourceType, SourceConfig, create_wayfair_reviews_client
from typing import Dict, Any
from config.wayfair_dag_configs import  PRODUCT_REVIEWS
from airflow.decorators import dag
from typing import List
from dags.wayfair.common_etl import get_latest_folder
from services.notification_handler import send_failure_notification
from datetime import datetime
from services.wayfair_service import WayfairService

class WayfairProductReviewsDAG(BaseReviewsDAG):
    """DAG for extracting Wayfair product reviews using integrated request client"""
    
    def __init__(self):
        # Use the predefined PRODUCT_REVIEWS config and override with any additional config values
        source_config = PRODUCT_REVIEWS
        
        # Create the specialized reviews client
        source_client = create_wayfair_reviews_client(source_config)
        
        # Initialize the base class
        super().__init__(source_config, source_client)
        
        
        # Create DAG tasks
        self.create_dag_tasks()
        self.path = f'data/wayfair/{datetime.now().year}/{datetime.now().month}/{datetime.now().day}/product_reviews'
   
    def get_items_to_process(self, mode: str = 'all') -> List[Dict[str, Any]]:
        """Get product variations based on mode"""
        if mode == 'all':
            pass
        elif mode == 'failed':
            all_variations = WayfairService().get_product_variations(self.path, has_variations=False)
            success_variations = WayfairService().get_success_product_variations(self.path, has_variations=False)
            failed_variations = WayfairService().get_failed_product_variations(all_variations, success_variations, has_variations=False)
            return [{'sku': item,
                     'reviews_per_page': 5000, 
                     'review_pages_total': None,
                     'url': f'https://www.wayfair.com/graphql',
                     'params': {'hash': 'a636f23a2ad15b342db756fb5e0ea093'}
                     
                     } for item in failed_variations]
        else:
            raise ValueError(f"Unsupported mode: {mode}")

    def build_file_name(self, metadata):
        """Build file name for reviews data"""
        import logging
        sku = metadata.get('sku', 'unknown')
        page_number = metadata.get('page_number', 1)
        reviews_per_page = metadata.get('reviews_per_page', 5000)
        filename = f"{sku}_{page_number}_{reviews_per_page}.json"
        return filename



@dag(
    dag_id='wayfair.get_product_reviews',
    description='Get product reviews from Wayfair',
    schedule_interval= None, #'*/30 * * * *', #each 30 minutes
    start_date=datetime(2025, 6, 11, 0, 0, 0),
    catchup=False,
    on_failure_callback=send_failure_notification,
    # on_success_callback=send_success_notification, 
    max_active_runs=1,
)
def wayfair_get_product_reviews_dag():
    WayfairProductReviewsDAG()  

wayfair_get_product_reviews_dag_instance = wayfair_get_product_reviews_dag()
          
