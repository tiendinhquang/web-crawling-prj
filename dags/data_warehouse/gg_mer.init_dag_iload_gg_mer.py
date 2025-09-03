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

table_codes = ["arielbath.gmc_sku_performance"]


class DataProcessor:
    def __init__(self, table_config: dict):
        self.table_config = table_config
    def process_data(self):
        df = pd.DataFrame()
        return df

    def process_sku_visibility_data(self, process_date):
        business_date = process_date.strftime('%Y%m%d')
        file_name = f'data/gg_merchants/sku_visibility/{self.table_config["filename"]}_{business_date}.csv'
        # file_name = 'data/gg_merchants/sku_visibility/sku_visibility_20250826_historical.csv'
        df = pd.read_csv(file_name, skiprows=2)
        df = df.dropna(subset=["Product ID", "Product title"])

        return df
                    
class CriteoDBLoader(BaseDBLoadDAG):
    def __init__(self, table_code: str):
        super().__init__(table_code)
    
        
    def process_data(self):
        data_processor = DataProcessor(self.table_config)
        if self.table_code == 'arielbath.gmc_sku_performance':
            df = data_processor.process_sku_visibility_data(datetime.now())
        else:
            df = data_processor.process_data()
        return df
    
    def load_data(self, df: pd.DataFrame):
        logging.info(f"Loading data with shape {df.shape}")
        self.data_loader.iload_to_db(self.table_code, f'tmp_{self.table_config["des_table"]}', df)


# Create DAGs for each table code
def make_criteo_dag(table_code: str):
    @dag(
        dag_id=f'gg_merchants.dag_iload_{table_code.split(".")[1]}',
        tags=["gg_merchants", 'data warehouse', 'iload'],
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
    globals()[f'criteo_dag_{code.replace(".", "_")}'] = make_criteo_dag(code)
