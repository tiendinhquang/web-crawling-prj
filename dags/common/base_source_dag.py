"""
Enhanced base source DAG that integrates with existing error handler and utils.

This combines the multi-source architecture with your existing error handler and utils
for maximum code reuse and consistency.
"""

import asyncio
import logging
import time
from typing import Dict, Any, List, Optional, Tuple
from abc import ABC, abstractmethod
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
from services.notification_handler import send_failure_notification

# Import existing utils and error handler
from utils.common.proxy_manager import get_working_proxies_sync
from services.error_handler import create_error_handler, UniversalErrorHandler

# Import enhanced components
from services.request_client import SourceType, SourceConfig, BaseSourceClient


class BaseSourceDAG(ABC):
    """
    Enhanced base DAG class for all data sources.
    
    Integrates with existing error handler and utils for maximum code reuse.
    """
    
    def __init__(self, source_config: SourceConfig,
                 source_client: BaseSourceClient):
        self.source_config = source_config
        self.source_client = source_client
        self.source_type = source_config.source_type
        
        # Use existing error handler with source-specific configuration
        self.error_handler = create_error_handler(source=source_config.source_type.value)
        
    @abstractmethod
    def get_items_to_process(self, mode: str = 'all') -> List[Any]:
        """
        Get items to process based on mode.
        
        Args:
            mode: Processing mode ('all', 'failed', etc.)
            
        Returns:
            List of items to process
        """
        pass
    

    def chunk_list(self, items: List[Any], chunk_size: int) -> List[List[Any]]:
        """Split items into chunks"""
        return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
    
    @abstractmethod
    def build_file_name(self, metadata):
        pass
    def save_results(self, results: List[Any]):
        import os
        import json
        from datetime import datetime 
        year, month, day = datetime.now().year, datetime.now().month, datetime.now().day
        # year,month,day = 2025,8,7
        path = f'data/{self.source_config.from_src}/{year}/{month}/{day}/{self.source_config.base_path}'
        try:

            os.makedirs(path, exist_ok=True) 
            for item in results:
                data, metadata = item
                file_name = self.build_file_name(metadata)
                file_path = os.path.join(path, file_name)
                with open(file_path, 'w') as f:
                    json.dump(data, f, indent=4)
                logging.info(f"✅ Product detail saved to {file_path}")
        except Exception as e:
            logging.error(f"❌ Error saving product detail: {e}")
            raise e
    
    
    def create_dag_tasks(self):
        """Create the standard DAG task structure with enhanced error handling"""
        
        @task.branch
        def choose_processing_mode(dag_run=None):
            """Choose processing mode based on DAG run configuration"""
            mode = dag_run.conf.get('mode', 'failed')
            logging.info(f"🚦 Processing mode: {mode}")
            
            if mode == 'all':
                return 'get_all_items'
            elif mode == 'failed':
                return 'get_failed_items'
            else:
                raise ValueError(f"Unsupported mode: {mode}")
        
        @task
        def get_all_items():
            """Get all items to process"""
            return self.get_items_to_process(mode='all')
        
        @task
        def get_failed_items():
            """Get failed items to retry"""
            return self.get_items_to_process(mode='failed')
        
        @task
        def process_items_batch(items: List[Any]):
            """
            Process items in batches with enhanced retry logic using existing error handler.
            """
            # Use existing proxy manager
            proxies = get_working_proxies_sync(limit=self.source_config.num_proxies)
            all_results = []
            all_failed = []
            
            # Log configuration
            max_concurrent = self.source_config.max_concurrent_requests
            batch_size = self.source_config.batch_size
            max_failures = self.source_config.max_failures_per_batch
            batch_delay = self.source_config.batch_delay_seconds
            
            logging.info(f"🚦 Configuration: max_concurrent={max_concurrent}, batch_size={batch_size}")
            logging.info(f"🌐 Source: {self.source_type.value}")
            logging.info(f"🔧 Error Handler: {self.source_type.value}-specific configuration")
            
            batches = self.chunk_list(items, batch_size)
            logging.info(f"🚀 Total batches: {len(batches)}")
            
            try:
                for i, batch in enumerate(batches):
                    logging.info(f"🚀 Processing batch #{i+1}/{len(batches)} with {len(batch)} items")
                    try:
                        # Process batch using enhanced client
                        # Let the request client create its own semaphore to avoid event loop binding issues
                        
                        batch_results = asyncio.run(self.source_client.process_batch(
                            items=batch,
                            proxies=proxies
                        ))
                        
                        # Track results
                        successful = [r for r in batch_results if r[1] is not None]
                        failed = [r[0] for r in batch_results if r[1] is None]
                        
                        if successful:
                            self.save_results(successful)
                            all_results.extend(successful)

                        all_failed.extend(failed)

                        
                        logging.info(f"✅ Batch #{i+1} - Success: {len(successful)}, Failed: {len(failed)}")
                        # Check failure threshold
                        if len(failed) >= max_failures or len(successful) == 0:
                            logging.error(f"❌ Batch #{i+1} exceeded failure threshold: {len(failed)}/{max_failures}")
                            raise Exception(f"Too many failures in batch #{i+1}: {len(failed)}/{max_failures}")
                        
                        # Delay between batches
                        if i < len(batches) - 1:
                            logging.info(f"⏳ Waiting {batch_delay} seconds before next batch...")
                            time.sleep(batch_delay)
                            
                    except Exception as e:
                        logging.error(f"❌ Batch #{i+1} failed: {e}")
                        raise e
                
                # Log error summary using client's error handler (where actual errors are tracked)
                error_summary = self.source_client.error_handler.get_error_summary()
                logging.info(f"📊 Error Summary: {error_summary}")
                
                logging.info(f"✅ Total processing complete - Success: {len(all_results)}, Failed: {len(all_failed)}")
                return {
                    'source': self.source_type.value,
                    'successful': len(all_results),
                    'failed': len(all_failed),
                    'total': len(items),
                    'error_summary': error_summary
                }
                
            except Exception as e:
                # Always log error summary even when processing fails
                error_summary = self.source_client.error_handler.get_error_summary()
                logging.error(f"📊 Error Summary on failure: {error_summary}")
                logging.error(f"❌ Processing failed: {e}")
                raise e
        

        # Create task dependencies
        choose = choose_processing_mode()
        all_vars = get_all_items()
        failed_vars = get_failed_items()

        # Set upstream dependencies
        all_vars.set_upstream(choose)
        failed_vars.set_upstream(choose)

        # Create batch processing tasks
        run_all = process_items_batch.override(task_id="run_batch_all")(all_vars)
        run_failed = process_items_batch.override(task_id="run_batch_failed")(failed_vars)

        run_all.set_upstream(all_vars)
        run_failed.set_upstream(failed_vars)
        return choose, all_vars, failed_vars, run_all, run_failed   



#examples


# from services.request_client import SourceType, SourceConfig, WayfairApiType
# from dags.common.base_source_dag import BaseSourceDAG
# class WayfairProductDAG(BaseSourceDAG):
#     def get_items_to_process(self, mode: str = 'all'):
#         if mode == 'all':
#             return [
#             {
#                 'sku': 'SKU001',
#                 'selected_options': ['1574842884'],
#                 'url': 'https://www.wayfair.com/graphql',
#                 'method': 'POST'
#             },
#             {
#                 'sku': 'SKU002',
#                 'selected_options': ['11111111'],
#                 'url': 'https://www.wayfair.com/graphql',
#                 'method': 'POST'
#             }
#         ]

#         elif mode == 'failed':
#             return [
#                 {
#                     'sku': 'SKU002',
#                     'selected_options': ['11111111'],
#                     'url': 'https://www.wayfair.com/graphql',
#                     'method': 'POST'
#                 }
#             ]  # giả lập
#         else:
#             raise ValueError(f"Unsupported mode: {mode}")
#     def save_results(self, results):
#         pass
# source_config = SourceConfig(
#     source_type=SourceType.WAYFAIR,
#     api_url='https://www.wayfair.com/graphql',
#     api_hash='3aa758d5f25f12aad0e6d873740bc223#45971694f177431f7253b94f54baed89#12ce0c12f8e7d25c55398081aa2e7e04',
#     headers_name='wayfair_product_info',
#     cookies_name='wayfair_product_info',
#     api_type=WayfairApiType.PRODUCT_INFO.value,
#     batch_size=10,
#     num_proxies=5,
#     max_concurrent_requests=3,
#     max_failures_per_batch=2,
#     batch_delay_seconds=1
# )

# dag_instance = create_source_dag(
#     source_type=SourceType.WAYFAIR,
#     dag_id='wayfair.test_info',
#     description='Get Wayfair product info by SKU',
#     source_config=source_config,
#     dag_class=WayfairProductDAG
# )


