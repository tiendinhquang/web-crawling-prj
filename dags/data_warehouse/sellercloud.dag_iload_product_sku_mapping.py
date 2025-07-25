from dags.data_warehouse.base_wayfair_db_load import BaseWayfairDBLoad
from utils.common.file_loader import read_csv
from airflow.decorators import dag
import pandas as pd
import logging
from utils.common.metadata_manager import get_latest_folder
from dags.notification_handler import send_success_notification, send_failure_notification
import json
from utils.common.file_loader import read_csv
from pathlib import Path
import os 
class SellercloudDBLoaderProductSkuMapping(BaseWayfairDBLoad):
    def __init__(self):
        super().__init__('core.product_sku_mapping')



    
    def process_data(self):
        pass
                
    
    def get_data(self):
        def _get_latest_file(root):
            files = list(Path(root).glob('*.csv'))  # ép về list để có thể kiểm tra
            if not files:
                raise FileNotFoundError(f"No CSV files found in: {root}")
            latest_file = max(files, key=os.path.getmtime)
            logging.info(f"the latest file is {latest_file}")
            return latest_file
        root = 'data/sellercloud/output/product_sku_mapping/'
        df = read_csv(_get_latest_file(root))
        df['from_src'] = 'sellercloud'
    
 
        return df
    def load_data(self, df: pd.DataFrame):
        self.data_loader.iload_to_db('core.product_sku_mapping', 'tmp_product_sku_mapping', df)
       

@dag(
    dag_id='sellercloud.dag_iload_product_sku_mapping',
    schedule_interval=None,
    start_date=None,
    catchup=False,
    on_failure_callback=send_failure_notification
)
def sellercloud_dag_iload_product_sku_mapping():
    dag_instance = SellercloudDBLoaderProductSkuMapping()
    return dag_instance.create_dag()

sellercloud_dag_iload_product_sku_mapping_instance = sellercloud_dag_iload_product_sku_mapping()