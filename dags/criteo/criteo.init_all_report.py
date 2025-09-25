from dags.common.base_source_dag import BaseSourceDAG
from services.request_client import SourceConfig, create_source_client, SourceType, CriteoApiType
from services.criteo_service import CriteoService
from services.notification_handler import send_failure_notification
import json
import os
import logging  
from config.criteo_dag_configs import CRITEO_CAPOUT_CONFIG, SEARCH_TERM_CONFIG, PLACEMENT_CONFIG, BID_MULTIPLIER_CONFIG, CAMPAIGN_CONFIG, LINE_ITEM_CONFIG
from datetime import datetime, timedelta
import asyncio

class CriteoGetBidMultiplierReport(BaseSourceDAG):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = f'data/criteo/{datetime.now().year}/{datetime.now().month}/{datetime.now().day}/bid_multiplier'
        os.makedirs(self.path, exist_ok=True)

    def get_items_to_process(self, mode):
        if mode == 'all':
            line_item_ids = CriteoService().get_line_item_ids()
            return [
                {
                    'line_item_id': item['id'],
                    'url': f'https://retailmedia.criteo.com/gateway/v2/auction-ads/line-items/{item["id"]}/sponsored-products',
                    'method': 'GET',
                    'payload': None,
                    'params': {}
                }
                for item in line_item_ids
            ]
        elif mode == 'failed':
            line_item_ids = [item['id'] for item in CriteoService().get_line_item_ids()]
            processed_line_item_ids = CriteoService().get_processed_line_item_ids(
                line_item_ids, datetime.now(), 'bid_multiplier'
            )
            all_items = [
                {
                    'line_item_id': line_item_id,
                    'url': f'https://retailmedia.criteo.com/gateway/v2/auction-ads/line-items/{line_item_id}/sponsored-products',
                    'method': 'GET',
                    'payload': None,
                    'params': {}
                }
                for line_item_id in line_item_ids
            ]
            failed_items = [item for item in all_items if item['line_item_id'] not in processed_line_item_ids]
            return failed_items
        else:
            raise ValueError(f"Unsupported mode: {mode}")

    def build_file_name(self, metadata):
        line_item_id = metadata.get('line_item_id')
        return f"{line_item_id}_bid_multiplier.json"
    def refresh_criteo_token(self):
        asyncio.run(CriteoService().refresh_token_and_update_headers())




from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

# Define the DAG types and their configurations
dag_types = [
    {
        'dag_id': 'criteo.dag_get_bid_multiplier_report',
        'name': 'bid_multiplier',
        'config': BID_MULTIPLIER_CONFIG,
        'class': CriteoGetBidMultiplierReport
    },

]
# Create DAGs dynamically using a for loop
def make_criteo_dag(dag_cfg: dict):
    @dag(
        dag_id=dag_cfg["dag_id"],
        tags=["criteo", dag_cfg["name"]],
        schedule_interval=None,
        start_date=days_ago(1),
        catchup=False,
        on_failure_callback=send_failure_notification
    )
    def _dag():
        source_client = create_source_client(SourceType.CRITEO, dag_cfg['config'])
        dag_instance = dag_cfg['class'](dag_cfg['config'], source_client)
        
        # Add token refresh task as the first step
        @task
        def refresh_token():
            dag_instance.refresh_criteo_token()
            return "Criteo token refreshed successfully"
        
        # Get the main DAG tasks
        choose, all_vars, failed_vars, run_all, run_failed = dag_instance.create_dag_tasks()
        
        # Set up task dependencies: refresh first → choose → main tasks
        refresh_task = refresh_token()
        refresh_task >> choose
        choose >> all_vars >> run_all
        choose >> failed_vars >> run_failed

        # Return tasks for DAG reference
        return refresh_task, choose, all_vars, failed_vars, run_all, run_failed
    
    return _dag()


for dag_cfg in dag_types:
    globals()[f'criteo_dag_{dag_cfg["name"].replace(".", "_")}'] = make_criteo_dag(dag_cfg)



