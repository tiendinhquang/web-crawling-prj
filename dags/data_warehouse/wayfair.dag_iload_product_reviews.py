from dags.data_warehouse.base_db_load_dag import BaseDBLoadDAG
from utils.common.db_loader.data_reader import DataReader
import json
import os
import pandas as pd 
from utils.common.metadata_manager import get_latest_folder
from airflow.decorators import dag
from services.notification_handler import send_failure_notification
from utils.common.file_loader import read_csv
class WayfairProductReviewsDBILoad(BaseDBLoadDAG):
    def __init__(self):
        super().__init__('wayfair.product_reviews')
        
    def process_reviews_data(self, base_path):
        all_reviews = []
        results = []
        for folder in os.listdir(base_path):
            sku = os.path.basename(folder)
            all_reviews = []
            rating_count = 0
            folder_path = os.path.join(base_path, folder)
            for file in os.listdir(folder_path):
                if file.endswith('.json'):
                    file_path = os.path.join(folder_path, file)
                    
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        reviews = data.get('data', {}).get('product', {}).get('customerReviews', {}).get('reviews', [])
                        if rating_count == 0:
                            rating_count= data.get('data', {}).get('product', {}).get('customerReviews', {}).get('ratingCount', 0)
                
                        all_reviews.extend(reviews)
            
            results.append(
            {
                'sku': sku,
                'rating_count': rating_count,
                'all_reviews': all_reviews
            }

            )
        return results 
    def load_to_csv(self, path, data):
        import pandas as pd
        import re
        all_data = []
        mismatched_skus = []

        for item in data:
            sku = item['sku']
            rating_count = item['rating_count']
            all_reviews = item['all_reviews']
            from_src = 'wayfair'

            if len(all_reviews) != rating_count:
                mismatched_skus.append({
                    'sku': sku,
                    'rating_count': rating_count,
                    'actual_reviews': len(all_reviews)
                })

            for review in all_reviews:
                review_data = {
                    'frm_src': from_src,
                    'sku': sku,
                    'rating_count': rating_count,
                    'reviewId': review.get('reviewId', ''),
                    'reviewerName': review.get('reviewerName', ''),
                    'isUSReviewer': review.get('isUSReviewer', False),
                    'ratingStars': float(review.get('ratingStars', 0) / 2),
                    'date': review.get('date', ''),
                    'productComments': review.get('productComments', ''),
                    'languageCode': review.get('languageCode', ''),
                    'reviewerLocation': review.get('reviewerLocation', ''),
                    'variationName': { option['name']: option['value'] for option in review.get('options', [])},
                }


                for option in review.get('options', []):
                    option_name = option.get('name')
                    option_value = option.get('value')
                    if option_name:
                        review_data[option_name] = option_value

                all_data.append(review_data)

        df = pd.DataFrame(all_data)
        df.columns = [re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', col).lower() for col in df.columns]
        df.to_csv(path, index=False, encoding='utf-8')
        print("Data has been successfully written to reviews_data.csv")

        if mismatched_skus:
            print("\nSKUs with mismatched review count:")
            for m in mismatched_skus:
                print(f"SKU: {m['sku']} | rating_count: {m['rating_count']} | actual_reviews: {m['actual_reviews']}")
        else:
            print("\nAll SKUs have matching review counts.")

    def process_data(self,):
        root = get_latest_folder(base_dir='data/wayfair')
        reviews_data = self.process_reviews_data(root + '/product_reviews', )
        self.load_to_csv(root + '/output/reviews_data.csv', reviews_data)
   

    
    def get_data(self):
        df = read_csv(get_latest_folder(base_dir='data/wayfair') + '/output/reviews_data.csv')
        df['from_src'] = 'wayfair'
        return df
        
        
    
    def load_data(self, df: pd.DataFrame):
        self.data_loader.iload_to_db('wayfair.product_reviews', 'tmp_product_reviews', df)

@dag(
    dag_id='wayfair.dag_iload_product_reviews',
    schedule_interval=None,
    start_date=None,
    catchup=False,
    on_failure_callback=send_failure_notification
)
def wayfair_dag_iload_product_reviews():
    dag_instance = WayfairProductReviewsDBILoad()
    return dag_instance.create_dag()
dag_instance = wayfair_dag_iload_product_reviews()
