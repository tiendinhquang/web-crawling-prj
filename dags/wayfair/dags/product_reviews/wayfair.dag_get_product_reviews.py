from dags.wayfair.dags.product_reviews.base_wayfair_product_reviews_api import BaseWayfairProductReviewsAPIDAG
from typing import Dict, Any
from config.wayfair_dag_configs import get_dag_config
from airflow.decorators import dag
from typing import List
import dags.wayfair.common_etl as etl
from dags.wayfair.common_etl import get_latest_folder
from dags.notification_handler import send_failure_notification
from datetime import datetime
class WayfairProductReviewsDAG(BaseWayfairProductReviewsAPIDAG):
    """DAG for extracting Wayfair product reviews"""
    def __init__(self, config: Dict):
        super().__init__(config)
        self.create_dag_tasks()

   
    def build_request_payload(self, sku: str, page_number: int, reviews_per_page: int) -> Dict[str, Any]:
        return {   'variables': {
                'sku': sku,
                'sort_order': 'DATE_DESCENDING',
                'page_number': page_number,
                'filter_rating': '',
                'reviews_per_page': reviews_per_page,
                'search_query': '',
                'language_code': 'en',}
        }
    def get_variations_for_mode(self, mode: str) -> List[Dict[str, Any]]:
        """Get product variations based on mode"""
        if mode == 'all':
            variations =  ['MVP11521', 'W004038334', 'YOW1064', 'W005666540', 'W006903724', 'WDX12078', 'DUJM1398', 'DLN2676', 'W006242257', 'W004530938', 'MOE8590', 'W000718280', 'SOST2120', 'DLN2659', 'W002611617', 'WFBS1220']
            variations = [{'sku': item, 'reviews_per_page': 10000, 'review_pages_total': None} for item in variations]
            return variations
            # return ['W003544669', 'W002602309', 'W004305066', 'LFMF3540', 'LBPR5672', 'W002973834', 'W100069620', 'W100396293', 'IVYB2199', 'W001175769', 'W003572696', 'W007691209', 'W110091403']
        elif mode == 'failed':
            # all_variations = etl.get_product_variations(has_variations=False)
            # import json
            # from utils.common.db_loader.data_reader import DataReader
            # df = DataReader().get_new_reviews()
            # df.rename(columns={'display_sku': 'sku','new_review_count': 'reviews_per_page'}, inplace=True)
            
         
            # all_variations = df['sku'].to_list()
            # success_variations = etl.get_success_product_variations(
            #     path=get_latest_folder(base_dir='data/wayfair') + '/product_reviews', has_variations=False
            # )
            # failed_variations = etl.get_failed_product_variations(all_variations, success_variations, has_variations=False)

            # failed_df = df[df['sku'].isin(failed_variations)]
            # failed_df['review_pages_total'] = 1
            # failed_df = failed_df[['sku', 'reviews_per_page', 'review_pages_total']]


            all_variations = etl.get_product_variations('data/wayfair/2025/7/8/product_detail/product_detail_page', has_variations=False)
            success_variations = etl.get_success_product_variations('data/wayfair/2025/7/8/product_reviews', has_variations=False)
            failed_variations = etl.get_failed_product_variations(all_variations, success_variations, has_variations=False)
            return [{'sku': item, 'reviews_per_page': 3000, 'review_pages_total': None} for item in failed_variations]
        else:
            raise ValueError(f"Unsupported mode: {mode}")

  
    
base_config = get_dag_config('product_reviews')
config = {
    "dag_id": "wayfair.dag_get_product_reviews",
    "description": "Extract product reviews from Wayfair",
    **base_config
}

@dag(
    dag_id=config['dag_id'],
    description=config['description'],
    schedule_interval= None, #'*/30 * * * *', #each 30 minutes
    start_date=datetime(2025, 6, 11, 0, 0, 0),
    catchup=False,
    on_failure_callback=send_failure_notification,
    # on_success_callback=send_success_notification, 
    max_active_runs=1,
)
def wayfair_get_product_reviews_dag():
    WayfairProductReviewsDAG(config)  
wayfair_get_product_reiviews_dag_instance = wayfair_get_product_reviews_dag()
          
