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

table_codes = ['lowes.sponsored_product_capout', 'lowes.search_term_report', 'lowes.placement_report', 'lowes.campaigns_report', 'lowes.line_items_report', 'lowes.attributed_transaction_report']


class DataProcessor:
    def __init__(self, table_config: dict):
        self.table_config = table_config
    def process_data(self):
        df = pd.DataFrame()
        return df
    def _process_bid_multiplier(self,max_date, process_date):
        page_types = [
            {"pageTypeName": "home", "pageTypeId": 2},
            {"pageTypeName": "search", "pageTypeId": 1},
            {"pageTypeName": "productdetail", "pageTypeId": 6},
            {"pageTypeName": "deals", "pageTypeId": 9},
            {"pageTypeName": "category", "pageTypeId": 5}
        ]
        all_df = pd.DataFrame()
        year, month, day = process_date.year, process_date.month, process_date.day
        base_path = f'data/criteo/{year}/{month}/{day}/bid_multiplier'
        date = max_date.strftime('%Y-%m-%d')
        if not os.path.exists(base_path):
            logging.info(f"No bid multiplier data found in folder {base_path}")
            return pd.DataFrame()
        results = []
        for file in os.listdir(base_path):
            if file.endswith('.json'):
                file_path = os.path.join(base_path, file)
                # get file write timestamp
                file_mtime = os.path.getmtime(file_path)
                file_time = datetime.fromtimestamp(file_mtime)
                with open(file_path, 'r', encoding='utf-8') as f:
                    bid = json.load(f)
                default_bid_cpc = bid['details']['cpc']['amount'] if isinstance(bid['details']['cpc'], dict) else None
                bid_multipliers = bid['details']['bidMultipliers']
                line_item_id = bid['externalLineItemId']
                campaign_id = bid['campaignId']
                for page_type in page_types:     
                    multiplier = next((m for m in bid_multipliers if m['pageTypeId'] == page_type['pageTypeId']), None)
                    results.append({
                        'line_item_id': str(line_item_id),
                        'campaign_id': str(campaign_id),
                        'page_type_id': str(page_type['pageTypeId']),
                        'page_type_name': page_type['pageTypeName'],
                        'bid_multiplier': (
                            None if not multiplier or multiplier.get('bidMultiplier') == 1
                            else multiplier.get('bidMultiplier')
                        ),
                        'adjusted_bid': multiplier.get('adjustedBid', default_bid_cpc) if multiplier else default_bid_cpc,
                        'date': date
                    })
        if not results:
            logging.info(f"No bid multiplier data found for {process_date}")
            return pd.DataFrame()
        df = pd.DataFrame(results)
        df['date'] = pd.to_datetime(df['date'])
        return df
    def process_placement_data(self, process_date):
        year, month, day = process_date.year, process_date.month, process_date.day
        root = self.table_config['base_path'].format(year=year, month=month, day=day)
        business_date = process_date.strftime('%Y-%m-%d')
        file_name = f'data/criteo/{year}/{month}/{day}/placement/{self.table_config["filename"]}_{business_date}.csv'
        all_df = pd.read_csv(file_name)
        all_df = all_df.rename(columns=self.table_config['mapping_cols'])
        # Convert date column to datetime to ensure consistent data type
        all_df['date'] = pd.to_datetime(all_df['date'])
        all_df['campaign_id'] = all_df['campaign_id'].astype(str)
        all_df['line_item_id'] = all_df['line_item_id'].astype(str)
        all_df['page_type_id'] = all_df['page_type_id'].astype(str)

        
        max_date = pd.to_datetime(all_df['date'].max())
        bid_multiplier_df = self._process_bid_multiplier(max_date, process_date)

        if all_df.empty:
            return pd.DataFrame()
        if bid_multiplier_df.empty:
            logging.info(
                f"No bid multiplier data found for {process_date}, "
            )
            return all_df
        final_df = pd.merge(
            all_df,
            bid_multiplier_df[['date', 'campaign_id', 'line_item_id', 'bid_multiplier', 'adjusted_bid', 'page_type_id']],
            on=['date', 'campaign_id', 'line_item_id', 'page_type_id'],
            how='left'
        )
        return final_df

    def process_attributed_transaction_data(self, process_date):
        from services.criteo_service import CriteoService
        year, month, day = process_date.year, process_date.month, process_date.day

        business_date = process_date.strftime('%Y-%m-%d')
        file_name = f'data/criteo/{year}/{month}/{day}/attributed_transaction/{self.table_config["filename"]}_{business_date}.csv'
        df = pd.read_csv(file_name)
        df['Ad Delivery - Page Type'] = df['Ad Delivery - Page Type'].str.lower()
        pt_df = asyncio.run(CriteoService().get_page_types())   
        all_df = df.merge(pt_df, left_on='Ad Delivery - Page Type', right_on='page_type_name', how='left')
        return all_df
                    
class CriteoDBLoader(BaseDBLoadDAG):
    def __init__(self, table_code: str):
        super().__init__(table_code)
    
        
    def process_data(self):
        data_processor = DataProcessor(self.table_config)
        if self.table_code == 'lowes.placement_report':
            df = data_processor.process_placement_data(datetime.now())
        elif self.table_code == 'lowes.attributed_transaction_report':
            df = data_processor.process_attributed_transaction_data(datetime.now())
        else:
            df = data_processor.process_data()
        return df
    
    def load_data(self, df: pd.DataFrame):
        logging.info(f"Loading data with shape {df.shape}")
        self.data_loader.iload_to_db(self.table_code, f'tmp_{self.table_config["des_table"]}', df)


# Create DAGs for each table code
def make_criteo_dag(table_code: str):
    @dag(
        dag_id=f'criteo.dag_iload_{table_code.split(".")[1]}',
        tags=["criteo", 'data warehouse', 'iload'],
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
