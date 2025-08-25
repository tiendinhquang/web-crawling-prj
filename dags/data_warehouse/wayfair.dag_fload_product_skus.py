# wayfair.dag_fload_product_skus.py
from dags.data_warehouse.base_db_load_dag import BaseDBLoadDAG
from utils.common.file_loader import read_csv
import pandas as pd
from airflow.decorators import dag
class WayfairProductSkusDBLoad(BaseDBLoadDAG):
    def __init__(self):
        super().__init__('wayfair.product_skus')

    def process_data(self):
        pass

    def get_data(self):
        """
        Get data from the source
        """
        df = read_csv('data/wayfair/2025/6/4/output/competitor_product_info.csv')
        df['ownership'] = 'competitor'
        df['from_src'] = 'wayfair'
        return df
    
    def load_data(self, df: pd.DataFrame):
        """
        Load data from the source and save it to the database
        """
        df = self.get_data()
        self.data_loader.fload_to_db(df, 'wayfair.product_skus')


@dag(
    dag_id='wayfair.dag_fload_product_skus',
    schedule_interval=None,
    start_date=None,
    catchup=False
)
def wayfair_dag_fload_product_skus():
    dag_instance = WayfairProductSkusDBLoad()
    return dag_instance.create_dag()

wayfair_dag_fload_product_skus_instance = wayfair_dag_fload_product_skus()