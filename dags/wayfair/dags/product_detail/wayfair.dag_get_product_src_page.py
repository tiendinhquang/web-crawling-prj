import json
import os
import logging
import requests
from bs4 import BeautifulSoup
from datetime import timedelta, datetime
from airflow import DAG
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
from utils.common.metadata_manager import get_latest_folder
from utils.common.proxy_manager import get_working_proxies_sync
from utils.common.config_manager import get_header_config, get_cookie_config
from services.notification_handler import send_success_notification, send_failure_notification
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Tuple
import time
import random
import httpx
import asyncio
from config.wayfair_dag_configs import get_dag_config
from dags.wayfair.wayfair_error_handler import WayfairErrorHandler, RetryConfig, create_error_handler

default_args = {
    'owner': 'airflow',
    'retries': None,
    'retry_delay': timedelta(minutes=5)
}

class BaseWayfairProductSrcPageDAG(ABC):
    """Base class for Wayfair product detail extraction DAGs"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.error_handler = create_error_handler(profile=config['error_handler_profile'])
        
    def chunk_list(self, lst: List, size: int):
        """Split list into smaller chunks"""
        for i in range(0, len(lst), size):
            yield lst[i:i + size]
    
    @abstractmethod
    def get_skus(self) -> List[Tuple[str, str]]:
        """Get list of SKUs to process - must be implemented by subclasses"""
        pass
    
    def save_results(self, results: List[Tuple[str, str]]):
        """Save results to files by extracting SKU and variation from HTML <div>"""
        for result in results:

            if result is None:
                continue
            sku, html_data = result
            if not html_data:
                logging.warning(f"❌ SKU {sku}: No data received")
                continue
            # 🧠 Parse HTML to extract metadata
            soup = BeautifulSoup(html_data, 'html.parser')
            metadata_div = soup.find('div', id='metadata')
            if metadata_div:
                extracted_sku = metadata_div.get('data-sku', sku)
                selected_variation = metadata_div.get('data-variation', None)
            else:
                logging.warning(f"⚠️ Metadata not found in HTML for SKU {sku}")
                extracted_sku = sku
                selected_variation = None
                continue

            # 📁 Generate file path
            now = datetime.now()
            year, month, day = now.year, now.month, now.day
            year, month, day = 2025, 6, 19
            if selected_variation:
                file_path = f"data/wayfair/{year}/{month}/{day}/product_detail/product_src_page/{extracted_sku}_{selected_variation}.html"
            else:
                file_path = f"data/wayfair/{year}/{month}/{day}/product_detail/product_src_page/{extracted_sku}.html"

            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'w+', encoding='utf-8') as f:
                f.write(html_data)

            logging.info(f"✅ Product detail saved to {file_path}")

    async def _make_request(self, sku: str, variation: list, url: str, proxy: Dict) -> Tuple[str, Optional[Dict]]:
        """Get product detail for a single SKU"""
        headers = get_header_config(header_name=self.config['headers_name'])
        cookies = get_cookie_config(cookie_name=self.config['cookies_name'])
        params = '%2C'.join(variation)
        try:
            async with httpx.AsyncClient(proxies=proxy, timeout=15, follow_redirects=True) as client:
                response = await client.get(url, params=params, headers=headers, cookies=cookies)   
                if not (200 <= response.status_code < 400):
                    raise httpx.HTTPStatusError(
                    f"Non-success status code {response.status_code}",
                    request=response.request,
                    response=response
                    )
                    
                try:
                    html_data = response.text
                    injection = f'<div id="metadata" data-sku="{sku}" data-variation="{"_".join(variation)}" style="display:none;"></div>'
                    if '<head>' in html_data:
                        html_data = html_data.replace('<head>', f'<head>\n{injection}')
                    else:
                        html_data = injection + '\n' + html_data
                    return sku, html_data

                except Exception as e:
                    logging.error(f"❌ SKU {sku}: Failed to parse HTML: {e}")
                    return sku, None
        except Exception as e:
            logging.error(f"❌ Failed SKU {sku}: {e}")
            raise e
    async def make_request_with_retry(self, sku: str, variation: list, url: str, proxy: Dict) -> Tuple[str, Optional[Dict]]:
        """Make API request with retry logic"""
        return await self.error_handler.execute_with_retry(self._make_request, sku, variation, url, proxy)

    async def run_batch(self, skus_to_fetch: List[Tuple[str, list[str], str]], proxies: List[Dict]) -> List[Tuple[str, Optional[Dict]]]:
        """Process a batch of SKUs with rotating proxies"""
        tasks = []
        proxy_idx = 0

        for idx, (sku, url, variation) in enumerate(skus_to_fetch):
            if idx % 5 == 0 and idx > 0:
                proxy_idx += 1
                logging.info(f"[🔄] Switching to proxy #{proxy_idx + 1} after {idx} SKUs")

            proxy = proxies[proxy_idx % len(proxies)]
            logging.info(f"[🌐] Using proxy for SKU {sku}: {proxy}")
            tasks.append(self.make_request_with_retry(sku, variation, url, proxy))

        return await asyncio.gather(*tasks)

    def create_dag_tasks(self):
        """Create the DAG tasks structure"""
        @task
        def get_skus_task():
            return self.get_skus()

        @task
        def async_request_batch(skus: List[Tuple[str, str, list[str]]]):
            """
            _Target: Process SKUs in batches with retry logic
            _Logic: Process SKUs in batches with retry logic, number of batches processed can be configured in wayfair_dag_configs.py
            """
            proxies = get_working_proxies_sync(limit=20)
            all_results = []
            all_failed = []

            for i, batch in enumerate(self.chunk_list(skus, self.config['batch_size'])):
                logging.info(f"🚀 Processing batch #{i} with {len(batch)} SKUs")
                batch_results = asyncio.run(self.run_batch(batch, proxies))
                self.save_results(batch_results)
                
                successful = [r for r in batch_results if r[1] not in (None, {}, [], '')]
                failed = [(sku, url, variation) for sku, url, variation in batch if sku not in [s for s, d in successful]]
                
                all_results.extend(successful)
                all_failed.extend(failed)
                
                logging.info(f"✅ Total success in batch #{i}: {len(successful)}")
                if failed:
                    logging.warning(f"⚠️ Batch #{i} - Failed SKUs: {failed}")
                    if len(failed) >= self.config['max_failures_per_batch'] or len(failed) == len(batch):
                        logging.info(f'total success: {len(all_results)}')
                        raise Exception(f"❌ Too many failed SKUs: {len(failed)}")
                        
                logging.info(f"🔄 Sleeping for 20 seconds before next request")
                time.sleep(20)
              
            logging.info(f"✅ Total success: {len(all_results)}")
            return all_results

        # Create task dependencies
        skus = get_skus_task()
        results = async_request_batch(skus)
        
        return skus, results

class WayfairProductSrcPageDAG(BaseWayfairProductSrcPageDAG):
    """Implementation of Wayfair product detail extraction DAG"""
    
    def get_skus(self) -> List[Tuple[str, str]]:
        """
        _Target: Get product list from Wayfair.product_list
        _Logic: Get all skus from sharepoint, then check if the sku is already processed, if not, then get the product detail
        """
        from dags.wayfair.common_etl import get_product_variations, get_success_product_variations, get_failed_product_variations
        vars = get_product_variations(path='data/wayfair/2025/6/19/product_detail/product_detail_page/')
        success_vars = get_success_product_variations(path = 'data/wayfair/2025/6/19/product_detail/product_src_page/')
        failed_vars = get_failed_product_variations(vars, success_vars)
        final_vars = []
        for variation in failed_vars:
            sku = variation['sku']
            url = variation['url']
            variation = variation['options']
            final_vars.append((sku, url, variation))
    
        return final_vars



# Configuration for product detail DAG
base_config = get_dag_config('product_src_page')
config = {
    "dag_id": "wayfair.dag_get_product_src_page",
    "description": "Extract product details from Wayfair",
    **base_config
}

@dag(
    dag_id=config['dag_id'],
    description=config['description'],
    # schedule_interval='*/30 * * * *', ## EACH 30 MINUTES
    schedule_interval=None, 
    start_date=days_ago(1),
    catchup=False,
    on_failure_callback=send_failure_notification
)
def wayfair_get_product_detail_dag():
    """DAG for getting product details from Wayfair"""
    dag_instance = WayfairProductSrcPageDAG(config)
    return dag_instance.create_dag_tasks()

wayfair_get_product_detail_dag_instance = wayfair_get_product_detail_dag()
