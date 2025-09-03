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

table_codes = ["ggads.auction_insights_daily_search","ggads.auction_insights_daily_shopping","ggads.auction_insights_monthly_search","ggads.auction_insights_monthly_shopping","ggads.auction_insights_weekly_search","ggads.auction_insights_weekly_shopping"
]


class DataProcessor:
    def __init__(self, table_config: dict):
        self.table_config = table_config
    def process_data(self, process_date):
        file_name = f'{self.table_config["filename"]}_{process_date.strftime("%Y%m%d")}.csv'
        file_path = os.path.join(self.table_config["base_path"], file_name)
        df = pd.read_csv(file_path, skiprows=2)        
        for col in df.columns:
            # ép toàn bộ về string
            df[col] = df[col].astype(str)
            # chuẩn hóa dữ liệu
            df[col] = df[col].str.strip()               # bỏ khoảng trắng thừa
            df[col] = df[col].str.replace('< 10%', '9.99', regex=False)
            df[col] = df[col].str.replace('%', '', regex=False)
            df[col] = df[col].str.replace('--', '0', regex=False)
            # thử convert về số
            try:
                df[col] = pd.to_numeric(df[col], errors='raise')
                df[col] = df[col]/100
            except Exception as e:
                logging.info(f"Error converting column {col} to numeric: {e}")
                pass
        return df
    
    def _to_last_day_of_month(self, month_str):
        from calendar import monthrange
        month, year = month_str.split()
        year = int(year)
        month_num = pd.to_datetime(month, format="%B").month  # "April" -> 4
        last_day = monthrange(year, month_num)[1]
        return f"{year}/{month_num:02d}/{last_day:02d}"
    def process_auction_insights_monthly_data(self, process_date):
        df = self.process_data(process_date)
        df['Month'] = df['Month'].apply(self._to_last_day_of_month)
        return df
       

    
                    
class CriteoDBLoader(BaseDBLoadDAG):
    def __init__(self, table_code: str):
        super().__init__(table_code)
    
        
    def process_data(self):
        data_processor = DataProcessor(self.table_config)
        if self.table_code in ['ggads.auction_insights_monthly_search', 'ggads.auction_insights_monthly_shopping']:
            df = data_processor.process_auction_insights_monthly_data(datetime.now())
        else:
            df = data_processor.process_data(datetime.now())
        return df
    
    def load_data(self, df: pd.DataFrame):
        logging.info(f"Loading data with shape {df.shape}")
        self.data_loader.iload_to_db(self.table_code, f'tmp_{self.table_config["des_table"]}', df)


# Create DAGs for each table code
def make_criteo_dag(table_code: str):
    @dag(
        dag_id=f'gg_ads.dag_iload_{table_code.split(".")[1]}',
        tags=["gg_ads", 'data warehouse', 'iload'],
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
