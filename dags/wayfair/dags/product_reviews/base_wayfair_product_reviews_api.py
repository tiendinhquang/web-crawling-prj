import re
import json
import os 
import sys
import time
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from utils.common.metadata_manager import get_latest_folder
from utils.common.config_manager import get_header_config, get_cookie_config
from utils.common.proxy_manager import get_working_proxies_sync
from collections import defaultdict
import httpx
import asyncio
import pandas as pd 
from datetime import datetime
import random

from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
from config.wayfair_dag_configs import get_dag_config

# Import the error handler
# sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from services.error_handler import create_error_handler

class BaseWayfairProductReviewsAPIDAG(ABC):
    """Base class for Wayfair product reviews API"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.error_handler = create_error_handler(source='wayfair')

    def chunked(self, lst: List[str], size: int):
        """Utility: chia list thành các chunks nhỏ."""
        for i in range(0, len(lst), size):
            yield lst[i:i + size]

    @abstractmethod
    def build_request_payload(self, sku: str, page_number: int, reviews_per_page: int) -> Dict[str, Any]:
        """Build the API request payload for product reviews"""
        pass

    @abstractmethod
    def get_variations_for_mode(self, mode: str) -> List[str]:
        pass
    
    async def _make_single_api_request(self, sku: str, page_number: int, reviews_per_page: int, proxy: Dict) -> Dict[str, Any]:
        headers = get_header_config(header_name=self.config['headers_name'])
        cookies = get_cookie_config(cookie_name=self.config['cookies_name'])
        params = {'hash': self.config['api_hash']}
        payload = self.build_request_payload(sku, page_number, reviews_per_page)
        async with httpx.AsyncClient(proxies=proxy, timeout=self.config['timeout_seconds'], follow_redirects=True) as client:
            response = await client.post(self.config['api_url'], headers=headers, params=params, json=payload, cookies=cookies)
            if not (200 <= response.status_code < 400):
                raise httpx.HTTPStatusError(
                    f"Non-success status code {response.status_code}",
                    request=response.request,
                    response=response
                )
            if not isinstance(response.json(), dict):
                raise ValueError(f"Response JSON is not a dict")
            
            response_data = response.json()
            response_data['page_number'] = page_number
            return sku, response_data
        
    async def make_api_request_with_retry(self, sku: str, page_number: int, reviews_per_page: int, proxy: Dict, semaphore: asyncio.Semaphore) -> Dict[str, Any]:
        # Random sleep between 1-2 seconds before acquiring semaphore
        await asyncio.sleep(random.uniform(1, 2))
        
        async with semaphore:
            return await self.error_handler.execute_with_retry(
                self._make_single_api_request,
                sku,
                page_number,
                reviews_per_page,
                proxy
            )
            
    async def process_single_sku(self, sku: str, proxy: Dict, reviews_per_page: int, review_pages_total: int = None, semaphore: asyncio.Semaphore = None) -> Dict[str, Any]:
        """Process a single SKU"""
        results = []
        page = 1
        while True:
            sku, data = await self.make_api_request_with_retry(sku, page, reviews_per_page, proxy, semaphore)
            results.append((sku, data))
            if not data:
                results.append((sku, None))
                break
            if review_pages_total is None:
                review_pages_total = (
                    data.get("data", {})
                        .get("product", {})
                        .get("customerReviews", {})
                        .get("reviewPagesTotal")
                )
            review_pages_total = review_pages_total if isinstance(review_pages_total, int) else 0

            page += 1
            if page > review_pages_total:
                break
        return results
    def save_results(self,results:List[Tuple[str, Dict[str, Any]]]):
        for result in results:
            if result is None or result[1] is None:
                continue
            sku, data = result 
            reviews_per_page = 0 
            page_number = 1
            try:
                reviews_per_page = (
                    data.get("data", {})
                        .get("product", {})
                        .get("customerReviews", {})
                        .get("reviewsPerPage", 0)
                )
                page_number = data.get('page_number')
            except AttributeError:
                logging.warning(f"⚠️ SKU {sku}: Malformed data structure")
            if  data and reviews_per_page == 0:
                reviews = data.get('data', {}).get('product', {}).get('customerReviews', {}).get('reviews', [])
                reviews_per_page = len(reviews)
            try:
                now = datetime.now()
                year, month, day = now.year, now.month, now.day
                year, month, day = 2025, 7, 8

                folder_path = f'data/wayfair/{year}/{month}/{day}{self.config["output_data_path"]}{sku}'
                file_path = f'{folder_path}/{sku}_{page_number}_{reviews_per_page}.json'
                
                os.makedirs(folder_path, exist_ok=True)
                with open(file_path, 'a+', encoding='utf-8') as f:
                    json.dump(data, f, indent=2)
                logging.info(f"✅ Product detail saved to {file_path}")
            except Exception as e:
                logging.error(f"❌ Failed to save file for SKU {sku}: {e}")
    async def run_batch(self, skus: List[str], proxies: List[Dict], semaphore: asyncio.Semaphore) -> List[Tuple[str, Dict[str, Any]]]:
        """Run batch processing for multiple SKUs"""
        tasks = []
        proxy_idx = 0
        for idx, sku in enumerate(skus):
            if idx % 5 == 0 and idx > 0:
                proxy_idx += 1
                logging.info(f"[🔄] Switching to proxy #{proxy_idx + 1} after processing {idx} SKUs")
            proxy = proxies[proxy_idx % len(proxies)]
            logging.info(f"[🌐] Using proxy for sku {sku['sku']}: {proxy}")
            tasks.append(self.process_single_sku(sku['sku'], proxy, sku['reviews_per_page'], sku['review_pages_total'], semaphore))
        results = await asyncio.gather(*tasks)
        return results
    def create_dag_tasks(self):
        """Create the standard DAG tasks structure"""
        @task.branch
        def choose_variation_mode(dag_run=None):
            variation_mode = dag_run.conf.get('variation_mode', 'failed')
            logging.info(f"🚦 Variation mode: {variation_mode}")

            if variation_mode == 'all':
                return 'get_all_product_variations'
            elif variation_mode == 'failed':
                return 'get_failed_product_variations'
            else:
                raise ValueError(f"Unsupported variation_mode: {variation_mode}")
        @task
        def get_all_product_variations():
            return self.get_variations_for_mode('all')
        @task
        def get_failed_product_variations():
            return self.get_variations_for_mode('failed')

        @task
        def run_batch_requests(skus: List[str]):
            """Run batch processing for multiple SKUs"""
            batches = list(self.chunked(skus, self.config['batch_size']))
            all_results = []
            total_failed_records = []
            
            # Log semaphore configuration
            max_concurrent = self.config.get('max_concurrent_requests', 10)
            logging.info(f"🚦 Semaphore limit: {max_concurrent} concurrent requests")
            
            for idx, batch in enumerate(batches):
                logging.info(f"🚀 Starting batch {idx+1}/{len(batches)} with {len(batch)} URLs")
                proxies = get_working_proxies_sync(self.config['num_proxies'])
                
                # Create semaphore in the same event loop where it will be used
                semaphore = asyncio.Semaphore(max_concurrent)
                results = asyncio.run(self.run_batch(batch, proxies, semaphore))
                results = [item for sublist in results for item in sublist]
                self.save_results(results)
                all_results.extend(results)
                failed_records = [batch_skus[res[0]] for res in results if (res is None or res[1] is None) and res[0] in (batch_skus := {sku['sku']: sku for sku in batch})]
                total_failed_records.extend(failed_records)
                logging.warning(f"❌ Failed URLs in batch {idx+1}: {failed_records}, total failed records in batches: {len(total_failed_records)}")
                if len(failed_records)  >= self.config['max_failures_per_batch']:
                    raise Exception(f"❌ Batch {idx+1} has more than {self.config['max_failures_per_batch']} failed records. Stopping DAG!")
                if idx < len(batches) - 1:
                    logging.info(f"⏳ Waiting {self.config['batch_delay_seconds']} seconds before next batch...")
                    time.sleep(self.config['batch_delay_seconds'])

            error_stats = self.error_handler.get_error_summary()
            logging.info(f"📊 Error Statistics: {error_stats}")
            logging.info(f"✅ All batches completed. Total requests: {len(all_results)}")
            logging.warning(f"❌ Total failed records: {len(total_failed_records)}")
        


                # Create task dependencies
        choose = choose_variation_mode()
        all_vars = get_all_product_variations()
        failed_vars = get_failed_product_variations()

        # Set upstream dependencies
        all_vars.set_upstream(choose)
        failed_vars.set_upstream(choose)

        # Create batch processing tasks
        run_all = run_batch_requests.override(task_id="run_batch_all")(all_vars)
        run_failed = run_batch_requests.override(task_id="run_batch_failed")(failed_vars)

        run_all.set_upstream(all_vars)
        run_failed.set_upstream(failed_vars)
        return choose, all_vars, failed_vars, run_all, run_failed



        





    



   
        
