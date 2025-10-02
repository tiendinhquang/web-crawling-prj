from dags.common.base_source_dag import BaseSourceDAG
from services.request_client import SourceConfig, create_source_client, SourceType

from services.lowes_vendor_service import LowesVendorService
from services.notification_handler import send_failure_notification
import json
import os
import logging  
from config.lowes_vendor_dag_configs import LOWES_RTM_CONFIG
from services.lowes_vendor_service import LowesVendorService
import urllib.parse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import asyncio




        
class LowesGetRTMDeductionDetails(BaseSourceDAG):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = f'data/lowes/{datetime.now().year}/{datetime.now().month}/{datetime.now().day}/rtm'
        os.makedirs(self.path, exist_ok=True)
        self.lowes_vendor_service = LowesVendorService()
    def refresh_token(self):
        return asyncio.run(self.lowes_vendor_service.refresh_cookies_and_update_config())
    def get_items_to_process(self, mode):
        deductions = asyncio.run(self.lowes_vendor_service.get_rtm_deductions_list(datetime.now() - timedelta(days=14), datetime.now()))
        if mode == 'all':
            return [{'url': self.source_config.api_url,
                 'method': 'POST',
                 'payload': deduction,
                 'params': None,
                 'business_date': datetime.now().strftime("%Y%m%d"),
                 'tracking_number': deduction.get('trackingNumber')} 
                for deduction in deductions]
        
        if mode == 'failed':
            tracking_numbers = self.lowes_vendor_service.get_process_rtm_deductions(datetime.now())
            return [{'url': self.source_config.api_url,
                     'method': 'POST',
                     'payload': deduction,
                     'params': None,
                     'business_date': datetime.now().strftime("%Y%m%d"),
                     'tracking_number': deduction.get('trackingNumber')}
                    for deduction in deductions if deduction.get('trackingNumber') not in tracking_numbers]
        else:
            raise ValueError(f"Unsupported mode: {mode}")
    def process_rtm_deduction_details(self, process_date):
        import pandas as pd
        year,month,day = process_date.year, process_date.month, process_date.day
        base_path = f'data/lowes/{year}/{month}/{day}/rtm'
        os.makedirs(base_path, exist_ok=True)
        results = []
        for file in os.listdir(base_path):
            if file.endswith('.json'):
                with open(os.path.join(base_path, file), 'r') as f:
                    data = json.load(f)
                deduction = data['data']['rtmDeductionSearch']
                detail_list = data['data']['rtMdetailsList'] or []
                # merge deduction info với từng record trong detail_list
                merged_records = [
                    {**deduction, **{k: v for k, v in d.items() if k not in deduction}}
                    for d in detail_list
                ]

                results.extend(merged_records)
        
        return pd.DataFrame(results)
    
    def save_to_s3(self):
        from utils.s3 import S3Hook
        process_date = datetime.now()
        year,month,day = process_date.year, process_date.month, process_date.day
        if self.source_config.api_type == 'rtm':
            df = self.process_rtm_deduction_details(process_date)
            file_path = f'data/lowes/{year}/{month}/{day}/output/rtm_{process_date.strftime("%Y%m%d")}.csv'
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            df.to_csv(file_path, index=False)
            s3_hook = S3Hook()
            s3_hook.load_file(
                filename=file_path,
                key=f'prod/crawler/bronze/lowes/lowes_vendor/rtm/rtm_{process_date.strftime("%Y%m%d")}.csv',
                bucket_name='atlasusa-ap-southeast-1-bi-auto-bucket',
                replace=True,
            )
        else:
            logging.warning(f"Unsupported API type for save to s3: {self.source_config.api_type}")
            return False
        return True
    def build_file_name(self, metadata):
        file_name = f"{metadata.get('tracking_number')}_{metadata.get('business_date')}.json"
        return file_name
   
        




from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

def create_dag_report_instance(config: SourceConfig):
    """Create DAG instance with the given configuration"""
    logging.info(f"🔄 Creating DAG instance with config: {config}")
    source_client = create_source_client(SourceType.LOWES, config)
    dag_instance = LowesGetRTMDeductionDetails(config, source_client)
    
    @task
    def refresh_lowes_token():
        """Refresh Lowes bearer token before processing"""
        return dag_instance.refresh_token()
    @task(trigger_rule="none_failed_min_one_success")
    def save_report_to_s3():
        return dag_instance.save_to_s3()
    token_refresh = refresh_lowes_token()
    choose, all_vars, failed_vars, run_all, run_failed = dag_instance.create_dag_tasks()
    
    # Set token refresh as upstream dependency
    choose.set_upstream(token_refresh)
    s3_task = save_report_to_s3()
    s3_task.set_upstream([run_all, run_failed])
    
    return choose, all_vars, failed_vars, run_all, run_failed, s3_task

# Base DAG configuration to eliminate repetition


# DAG configurations
DAG_CONFIGS = [
    {
        'dag_id': 'lowes.dag_get_lowes_rtm_report',
        'tags': ["lowes", "vendor"],
        'description': 'Get Lowes RTM report',
        'config': LOWES_RTM_CONFIG,
        'schedule_interval': "0 9 * * *",
        'start_date': days_ago(1),
        'catchup': False,
        'max_active_runs': 1,
        'on_failure_callback': send_failure_notification
    }
   
]

# Generate DAGs dynamically
for dag_cfg in DAG_CONFIGS:
    @dag(
        dag_id=dag_cfg['dag_id'],
        tags=dag_cfg['tags'],
        description=dag_cfg['description'],
        schedule=dag_cfg['schedule_interval'],
        start_date=dag_cfg['start_date'],
        catchup=dag_cfg['catchup'],
        max_active_runs=dag_cfg['max_active_runs'],
        on_failure_callback=dag_cfg['on_failure_callback']
    )
    def generated_dag():
        return create_dag_report_instance(dag_cfg['config'])
    
    globals()[dag_cfg['dag_id']] = generated_dag()



