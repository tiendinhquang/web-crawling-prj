import os
import sys
import asyncio
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.common.db.connection import DBConnection
import pandas as pd
import logging  
from typing import Optional
import requests
from utils.common.config_manager import get_header_config, update_header_config
import yaml
from datetime import datetime, timedelta
import os
from services.request_client import SourceConfig, create_source_client, SourceType
from config.criteo_dag_configs import CRITEO_CAPOUT_CONFIG
with open('config/credentials.yaml', 'r') as f:
    cfg = yaml.safe_load(f)

TOKEN = cfg['token']

class CriteoService:
    def __init__(self):
        self.db_engine = DBConnection().engine
        self.headers_name = "criteo_capout"
        self.token_url = "http://172.17.2.54:8000/api/v1/criteo/bearer-token"
        self.base_api_url = "https://rm-reporting.criteo.net/dashboards/rm-dsp-analytics-open-auction-flexible/api"
        self.client = create_source_client(SourceType.CRITEO, CRITEO_CAPOUT_CONFIG)

    def get_campaign_ids(self):
        try:
            logging.info("🔍 Fetching campaign IDs from database...")
            from sqlalchemy import text
            # Check if database engine is available
            if not self.db_engine:
                logging.error("❌ Database engine is not available")
                return []
            
            q = """
                SELECT id
                FROM lowes.campaigns
                WHERE name NOT LIKE '%test%'
            """
            
            logging.info(f"🔍 Executing query: {q}")

            df = pd.read_sql_query(text(q), self.db_engine)
            
            if df.empty:
                logging.warning("⚠️ No campaigns found in database")
                return []
            
            campaign_ids = df['id'].tolist()
            logging.info(f"✅ Successfully fetched {len(campaign_ids)} campaign IDs")
            return campaign_ids
            
        except Exception as e:
            logging.error(f"❌ Error fetching campaign IDs: {e}")
            logging.error(f"❌ Error type: {type(e)}")
            logging.error(f"❌ Database engine type: {type(self.db_engine)}")
            raise e
    def get_line_item_ids(self):
        try:
            logging.info("🔍 Fetching line item IDs from database...")
            from sqlalchemy import text
            # Check if database engine is available
            if not self.db_engine:
                logging.error("❌ Database engine is not available")
                return []
            
            q = """
                SELECT DISTINCT id, campaign_id
                FROM lowes.line_items
                WHERE campaign_id IN (
                    SELECT id 
                    FROM lowes.campaigns 
                    WHERE LOWER(name) NOT LIKE '%test%'
                )
                AND LOWER(name) NOT LIKE '%test%';

            """
            
            logging.info(f"🔍 Executing query: {q}")

            df = pd.read_sql_query(text(q), self.db_engine)
            
            if df.empty:
                logging.warning("⚠️ No Line Item found in database")
                return []
            #convert to dict
            line_item_ids = df.to_dict(orient='records')
            logging.info(f"✅ Successfully fetched {len(line_item_ids)} line item IDs")
            return line_item_ids
            
        except Exception as e:
            logging.error(f"❌ Error fetching line item IDs: {e}")
            logging.error(f"❌ Error type: {type(e)}")
            logging.error(f"❌ Database engine type: {type(self.db_engine)}")
            raise e
    def get_processed_line_item_ids(self, line_item_ids: list, process_date: datetime, base_path: str):
        processed_line_item_ids = []
        year, month, day = process_date.year, process_date.month, process_date.day
        root = f'data/criteo/{year}/{month}/{day}/{base_path}'
        os.makedirs(root, exist_ok=True)
        file_names = [f'{id}_{base_path}.json' for id in line_item_ids]
        for file in os.listdir(root):
            if file.endswith('.json') and file in file_names:
                processed_line_item_ids.append(file.split('_')[0])
        return processed_line_item_ids
    
    
    def get_processed_campaign_ids(self, period: str, campaign_ids: list, process_date: datetime, base_path: str):
        processed_campaign_ids = []
        year,month,day = process_date.year, process_date.month, process_date.day
        root = f'data/criteo/{year}/{month}/{day}/{base_path}'
        os.makedirs(root, exist_ok=True)
        file_names = [f'{id}_{period}_{base_path}_report.json' for id in campaign_ids]
        for file in os.listdir(root):
            if file.endswith('.json') and file in file_names:
                processed_campaign_ids.append(file.split('_')[0])
        return processed_campaign_ids
    def get_processed_line_item_ids_by_date_range(self, process_date, start_date, end_date, base_path, line_item_ids, period):
        """
        Get processed line item IDs for date range processing.
        Returns a set of tuples (line_item_id, date_str) for items that have already been processed.
        """
        year, month, day = process_date.year, process_date.month, process_date.day
        root = f'data/criteo/{year}/{month}/{day}/{base_path}'
        os.makedirs(root, exist_ok=True)

        processed_line_item_dates = set()
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Read all existing files in the directory for optimization
        existing_files = set(os.listdir(root))

        for line_item in line_item_ids:
            line_item_id = line_item['id']
            for day_offset in range((end_dt - start_dt).days + 1):
                day_str = (start_dt + timedelta(days=day_offset)).strftime("%Y-%m-%d")
                expected_file = f"{line_item_id}_{day_str}_{base_path}.json"
                if expected_file in existing_files:
                    processed_line_item_dates.add((str(line_item_id), day_str))

        return list(processed_line_item_dates)

    def get_processed_campaign_ids_by_date_range(self, process_date, start_date, end_date, base_path, campaign_ids):
        year, month, day = process_date.year, process_date.month, process_date.day
        # year, month, day = 2025, 8, 15
        root = f'data/criteo/{year}/{month}/{day}/{base_path}'
        os.makedirs(root, exist_ok=True)

        processed_campaign_ids = set()
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Đọc toàn bộ file có trong thư mục để tối ưu
        existing_files = set(os.listdir(root))

        for campaign_id in campaign_ids:
            for day_offset in range((end_dt - start_dt).days + 1):
                day_str = (start_dt + timedelta(days=day_offset)).strftime("%Y-%m-%d")
                expected_file = f"{campaign_id}_{day_str}_{base_path}_report.json"
                if expected_file in existing_files:
                    processed_campaign_ids.add((str(campaign_id), day_str))

        return list(processed_campaign_ids)
    
    def get_processed_campaign_dates_by_date_range(self, process_date, start_date, end_date, base_path, campaign_ids):
        """
        Get processed campaign+date combinations for date range processing.
        Returns a set of strings in format "campaign_id_date" for items that have already been processed.
        """
        year, month, day = process_date.year, process_date.month, process_date.day
        root = f'data/criteo/{year}/{month}/{day}/{base_path}'
        os.makedirs(root, exist_ok=True)

        processed_campaign_dates = set()
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Read all existing files in the directory for optimization
        existing_files = set(os.listdir(root))

        for campaign_id in campaign_ids:
            for day_offset in range((end_dt - start_dt).days + 1):
                day_str = (start_dt + timedelta(days=day_offset)).strftime("%Y-%m-%d")
                expected_file = f"{campaign_id}_{day_str}_{base_path}_report.json"
                if expected_file in existing_files:
                    # Add campaign+date combination to processed set
                    processed_campaign_dates.add(f"{campaign_id}_{day_str}")

        return processed_campaign_dates
    async def _fetch_new_token(self) -> Optional[str]:
        """Fetch new bearer token from the API endpoint"""
        try:
            headers = {
                'accept': 'application/json',
                'Authorization': f'Bearer {TOKEN}'
            }
            
            # Create a semaphore for the request
            semaphore = asyncio.Semaphore(1)
            
            logging.info(f"Fetching new token from: {self.token_url}")
            response, metadata = await self.client.make_request_with_retry(
                self.token_url, 
                method='GET', 
                headers=headers,
                semaphore=semaphore
            )
            
            if metadata['response_status_code'] == 200:
                data = response
                if data.get('status') == 'success' and 'token' in data:
                    new_token = data['token']
                    logging.info("Successfully fetched new bearer token")
                    return new_token
                else:
                    logging.error(f"Invalid response format: {data}")
                    return None
            else:
                logging.error(f"Failed to fetch token. Status: {metadata['response_status_code']}, Response: {response}")
                return None
                
        except Exception as e:
            logging.error(f"Error fetching new token: {e}")
            return None
    
    async def _update_headers_with_new_token(self, new_token: str) -> bool:
        """Update header configuration with new bearer token"""
        try:
            current_headers = get_header_config(self.headers_name)
            
            # Update the authorization header with the new token
            updated_headers = current_headers.copy()
            updated_headers['authorization'] = new_token
            
            # Update the header configuration
            success = update_header_config(self.headers_name, updated_headers)
            
            if success:
                logging.info("Successfully updated header configuration with new token")
            else:
                logging.error("Failed to update header configuration")
                
            return success
            
        except Exception as e:
            logging.error(f"Error updating headers with new token: {e}")
            return False
    
    async def refresh_token_and_update_headers(self) -> bool:
        """Main method to refresh token and update headers"""
        try:
            logging.info("Starting token refresh process...")
            
            # Fetch new token
            new_token = await self._fetch_new_token()
            if not new_token:
                logging.error("Failed to fetch new token")
                return False
            
            # Update headers with new token
            success = await self._update_headers_with_new_token(new_token)
            
            if success:
                logging.info("Token refresh and header update completed successfully")
            else:
                logging.error("Token refresh completed but header update failed")
                
            return success
            
        except Exception as e:
            logging.error(f"Error in token refresh process: {e}")
            return False 

    async def create_report_by_date_range(self,report_type, start_date= None, end_date= None,dimensions= None, metrics= None, campaign_ids=None):
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        json_data = f'clientIds=464783722056458240&dateRange=en,America%2FNew_York,1,{start_date_str}_{end_date_str},PREVIOUS_PERIOD&dimensions={dimensions}&metrics={metrics}&rmAccountName=Atlas%2520International%252C%2520Inc&currencySymbol=%24&currencySymbolLeft=true'

        params = {
            "clientIds": "464783722056458240",
            "dateRange": f"en,America/New_York,1,{start_date_str}_{end_date_str},PREVIOUS_PERIOD",
            "dimensions": dimensions,
            "metrics": metrics,
            "rmAccountName": "Atlas International, Inc",
            "currencySymbol": "$",
            "currencySymbolLeft": "true",
            "version": "default"
        }
        semaphore = asyncio.Semaphore(1)

        if report_type == 'capout':
            url = f'https://rm-reporting.criteo.net/dashboards/rm-dsp-analytics-capout-flexible/api/exports/csv'
        elif report_type == 'attributed_transaction':
            json_data = f'clientIds=464783722056458240&dateRange=en,America%2FNew_York,1,{start_date_str}_{end_date_str},PREVIOUS_PERIOD&currency=USD&campaignIds={",".join(campaign_ids)}&rmAccountName=Atlas%2520International%252C%2520Inc&currencySymbol=%24&currencySymbolLeft=true'
            params = {  
                "clientIds": "464783722056458240",
                "dateRange": f"en,America/New_York,1,{start_date_str}_{end_date_str},PREVIOUS_PERIOD",
                "currency": "USD",
                "campaignIds": ",".join(campaign_ids),
                "rmAccountName": "Atlas International, Inc",
                "currencySymbol": "$",
                "currencySymbolLeft": "true",
                "version": "default"
            }
            url = f'https://rm-reporting.criteo.net/dashboards/rm-dsp-atl/api/exports/csv'
        else:
            url = f"{self.base_api_url}/exports/csv"
        
        response, metadata = await self.client.make_request_with_retry(
            url=url,
            method='POST',
            payload=json_data,
            params=params,
            semaphore=semaphore
        )
        logging.info(f"Report created successfully. ID: {response['id']}")
        return response['id']

    async def get_report_status(self, report_type, report_id= None, start_date= None, end_date= None, dimensions= None, metrics= None, campaign_ids=None):
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        params = {
            "clientIds": "464783722056458240",
            "dateRange": f"en,America/New_York,1,{start_date_str}_{end_date_str},PREVIOUS_PERIOD",
            "dimensions": dimensions,
            "metrics": metrics,
            "rmAccountName": "Atlas International, Inc",
            "currencySymbol": "$",
            "currencySymbolLeft": "true",
            "version": "default",
            "id": report_id
        }
        semaphore = asyncio.Semaphore(1)
        if report_type == 'capout':
            url = f'https://rm-reporting.criteo.net/dashboards/rm-dsp-analytics-capout-flexible/api/exports/csv'
        elif report_type == 'attributed_transaction':
            params = {
                "clientIds": "464783722056458240",
                "dateRange": f"en,America/New_York,1,{start_date_str}_{end_date_str},PREVIOUS_PERIOD",
                "currency": "USD",
                "campaignIds": ",".join(campaign_ids),
                "rmAccountName": "Atlas International, Inc",
                "currencySymbol": "$",
                "currencySymbolLeft": "true",
                "version": "default",
                "id": report_id
            }
            url = f'https://rm-reporting.criteo.net/dashboards/rm-dsp-atl/api/exports/csv'
        else:
            url = f"{self.base_api_url}/exports/csv"
        response, metadata = await self.client.make_request_with_retry(
            url=url,
            method='POST',
            params=params,
            semaphore=semaphore
        )
        
        return {
            'report_id': response['id'],
            'status': response['status']
        }
    async def get_page_types(self):
        import pandas as pd 
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        params = {
            "clientIds": "464783722056458240",
            "dateRange": f"en,America/New_York,1,{start_date_str}_{end_date_str},PREVIOUS_PERIOD",
            "dimensions": "page_type_name,page_type_id",
            "metrics": "total_spend",
            "currentDimension": "page_type_name",
            "cacheBust": "m40n1nv0uz9",
            "version": "default"
        }
        semaphore = asyncio.Semaphore(1)
        url = f'https://rm-reporting.criteo.net/dashboards/rm-dsp-analytics-open-auction-flexible/api/datasets/open-auction'
        response, metadata = await self.client.make_request_with_retry(
            url=url,
            method='GET',
            params=params,
            semaphore=semaphore
        )
        columns = response['columns']
        values = response['values']
        df = pd.DataFrame(values, columns=columns)
        df = df[['page_type_id','page_type_name']].drop_duplicates()
        return df
    async def save_report_data_to_local(self, report_type, report_id, start_date= None, end_date= None, dimensions= None, metrics= None, path= None, campaign_ids=None):
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        params = {
            "clientIds": "464783722056458240",
            "dateRange": f"en,America/New_York,1,{start_date_str}_{end_date_str},PREVIOUS_PERIOD",
            "dimensions": dimensions,
            "metrics": metrics,
            "rmAccountName": "Atlas International, Inc",
            "currencySymbol": "$",
            "currencySymbolLeft": "true",
            "id": report_id
        }
        semaphore = asyncio.Semaphore(1)
        if report_type == 'attributed_transaction':
            url = f'https://rm-reporting.criteo.net/dashboards/rm-dsp-atl/api/exports/download/'
            params = {
                "clientIds": "464783722056458240",
                "dateRange": f"en,America/New_York,1,{start_date_str}_{end_date_str},PREVIOUS_PERIOD",
                "currency": "USD",
                "campaignIds": ",".join(campaign_ids),
                "rmAccountName": "Atlas International, Inc",
                "id": report_id
            }
        else:
            url = f"{self.base_api_url}/exports/download/"
        response, metadata = await self.client.make_request_with_retry(
            url=url,
            method='GET',
            params=params,
            semaphore=semaphore
        )
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(response['text'])
        logging.info(f"Report data saved to local. Path: {path}")
    
if __name__ == "__main__":
    import asyncio
    criteo_service = CriteoService()
    # campaign_ids = criteo_service.get_campaign_ids()
    # asyncio.run(criteo_service.refresh_token_and_update_headers())
    # asyncio.run(criteo_service.create_report_by_date_range(report_type='attributed_transaction', start_date=datetime.now() - timedelta(days=14), end_date=datetime.now(), campaign_ids=campaign_ids))
    
    print(asyncio.run(criteo_service.get_page_types(start_date=datetime.now(), end_date=datetime.now())))