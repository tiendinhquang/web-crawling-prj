from dags.common.base_source_dag import BaseSourceDAG
from services.request_client import SourceConfig
from typing import List, Dict, Any, Tuple
import asyncio
import logging
import time
from airflow.decorators import task, dag
from utils.common.proxy_manager import get_working_proxies_sync

class BaseReviewsDAG(BaseSourceDAG):
    """
    Specialized base DAG for reviews that require pagination processing.
    This extends BaseSourceDAG to handle the unique requirements of reviews
    without affecting other DAG implementations.
    """
    
    def __init__(self, source_config: SourceConfig, source_client):
        super().__init__(source_config, source_client)
        
    def process_reviews_batch(
        self,
        items: List[Dict[str, Any]],
        proxies: List[Dict],
        semaphore: asyncio.Semaphore
    ) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Process reviews in batches with pagination support.
        This method now returns the items to be processed asynchronously by the task.
        """
        # Return the items with their processing parameters instead of processing them synchronously
        return items
    
    def create_dag_tasks(self):
        """Create the standard DAG task structure with enhanced error handling for reviews"""
        
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
        def process_reviews_batch(items: List[Any]):
            """
            Process reviews in batches with specialized pagination logic.
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
            
            logging.info(f"🚦 Reviews Configuration: max_concurrent={max_concurrent}, batch_size={batch_size}")
            logging.info(f"🌐 Source: {self.source_type.value}")
            logging.info(f"🔧 Error Handler: {self.source_type.value}-specific configuration")
            
            batches = self.chunk_list(items, batch_size)
            logging.info(f"🚀 Total batches: {len(batches)}")
            
            try:
                for i, batch in enumerate(batches):
                    logging.info(f"🚀 Processing reviews batch #{i+1}/{len(batches)} with {len(batch)} items")
                    try:
                        # Create semaphore for this batch
                        semaphore = asyncio.Semaphore(max_concurrent)
                        
                        # Process each SKU in the batch using the specialized reviews method
                        batch_results = []
                        for item in batch:
                            sku = item.get('sku')
                            if not sku:
                                logging.warning(f"⚠️ Skipping item without SKU: {item}")
                                continue
                            
                            try:
                                # Use asyncio.run() here like BaseSourceDAG does
                                sku_results = asyncio.run(
                                    self.source_client.process_reviews_with_pagination(
                                        proxy=proxies[i % len(proxies)] if proxies else None,
                                        semaphore=semaphore,
                                        **item
                                    )
                                )
                                batch_results.extend(sku_results)
                            except Exception as e:
                                logging.error(f"❌ Failed to process reviews for SKU {sku}: {e}")
                                # Create metadata for failed results
                                metadata = {
                                    'sku': sku,
                                    'page_number': 1,
                                    'reviews_per_page': item.get('reviews_per_page', 10000),
                                    'url': item.get('url'),
                                    'params': item.get('params')
                                }
                                batch_results.append((None, metadata))
                        
                        # Track results
                        successful = [r for r in batch_results if r[0] is not None]
                        failed = [r[1].get('sku', 'unknown') for r in batch_results if r[0] is None]
                        
                        if successful:
                            self.save_results(successful)
                            all_results.extend(successful)

                        all_failed.extend(failed)

                        logging.info(f"✅ Reviews batch #{i+1} - Success: {len(successful)}, Failed: {len(failed)}")
                        
                        # Check failure threshold
                        if len(failed) >= max_failures or len(successful) == 0:
                            logging.error(f"❌ Reviews batch #{i+1} exceeded failure threshold: {len(failed)}/{max_failures}")
                            raise Exception(f"Too many failures in reviews batch #{i+1}: {len(failed)}/{max_failures}")
                        
                        # Delay between batches
                        if i < len(batches) - 1:
                            logging.info(f"⏳ Waiting {batch_delay} seconds before next batch...")
                            time.sleep(batch_delay)
                            
                    except Exception as e:
                        logging.error(f"❌ Reviews batch #{i+1} failed: {e}")
                        raise e
                
                # Log error summary
                error_summary = self.source_client.error_handler.get_error_summary()
                logging.info(f"📊 Reviews Error Summary: {error_summary}")
                
                logging.info(f"✅ Total reviews processing complete - Success: {len(all_results)}, Failed: {len(all_failed)}")
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
                logging.error(f"📊 Reviews Error Summary on failure: {error_summary}")
                logging.error(f"❌ Reviews processing failed: {e}")
                raise e

        # Create task dependencies
        choose = choose_processing_mode()
        all_vars = get_all_items()
        failed_vars = get_failed_items()

        # Set upstream dependencies
        all_vars.set_upstream(choose)
        failed_vars.set_upstream(choose)

        # Create batch processing tasks
        run_all = process_reviews_batch.override(task_id="run_reviews_batch_all")(all_vars)
        run_failed = process_reviews_batch.override(task_id="run_reviews_batch_failed")(failed_vars)

        run_all.set_upstream(all_vars)
        run_failed.set_upstream(failed_vars)
        return choose, all_vars, failed_vars, run_all, run_failed

