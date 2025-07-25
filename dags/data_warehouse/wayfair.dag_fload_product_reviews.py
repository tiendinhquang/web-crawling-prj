from dags.data_warehouse.base_wayfair_db_load import BaseWayfairDBLoad
from utils.common.file_loader import read_csv
import pandas as pd
from airflow.decorators import dag

class WayfairProductReviewsDBFLoad(BaseWayfairDBLoad):
    def __init__(self):
        super().__init__('wayfair.product_reviews')
    def process_data(self):
        pass

    def get_data(self):
        df = read_csv('data/wayfair/2025/6/11/output/reviews_data.csv')
        df['from_src'] = 'wayfair'

        processed_data = ['W009037857', 'W009360477', 'W009698251', 'W009782013', 'W009360438', 'W009632254', 'W009360679', 'W009629064', 'W009360311', 'W009624685', 'W009360313', 'W009463181', 'W009360325', 'W009360671', 'WDX12051', 'WDX12055', 'W009898370', 'W009898036', 'W011221106', 'W011221153', 'W009401827', 'W100406133', 'W100454812', 'W011221211', 'W009400326', 'W009400846', 'W011221298', 'W100454818', 'WDX12635', 'WDX12632', 'W009400824', 'W011218891', 'W009037857']
        df = df[df['sku'].isin(processed_data)]
        return df
    def load_data(self, df: pd.DataFrame):
        self.data_loader.fload_to_db(df, 'wayfair.product_reviews')


@dag(
    dag_id='wayfair.dag_fload_product_reviews',
    schedule_interval=None,
    start_date=None,
    catchup=False
)
def wayfair_dag_fload_product_reviews():
    dag_instance = WayfairProductReviewsDBFLoad()
    return dag_instance.create_dag()
wayfair_dag_fload_product_reviews_instace=wayfair_dag_fload_product_reviews()