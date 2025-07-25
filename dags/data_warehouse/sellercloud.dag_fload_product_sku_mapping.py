# wayfair.dag_fload_product_skus.py
from dags.data_warehouse.base_wayfair_db_load import BaseWayfairDBLoad
from utils.common.file_loader import read_csv
import pandas as pd
from airflow.decorators import dag
class SellercloudProductSkuMappingDBLoad(BaseWayfairDBLoad):
    def __init__(self):
        super().__init__('core.product_sku_mapping')

    def process_data(self):
        pass

    def get_data(self):
        """
        Get data from the source
        """
        df = read_csv('data/sellercloud/output/20250703063435.csv')
        df['from_src'] = 'sellercloud'

 
        return df
    
    def load_data(self, df: pd.DataFrame):
        """
        Load data from the source and save it to the database
        """
        df = self.get_data()
        self.data_loader.fload_to_db(df, 'core.product_sku_mapping')


@dag(
    dag_id='sellercloud.dag_fload_product_sku_mapping',
    schedule_interval=None,
    start_date=None,
    catchup=False
)
def sellercloud_dag_fload_product_sku_mapping():
    dag_instance = SellercloudProductSkuMappingDBLoad()
    return dag_instance.create_dag()

sellercloud_dag_fload_product_sku_mapping_instance = sellercloud_dag_fload_product_sku_mapping()