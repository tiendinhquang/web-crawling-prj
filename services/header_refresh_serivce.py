
import requests
import logging
import yaml
from typing import Dict, Any, Optional, List
from utils.common.config_manager import get_header_config, update_header_config
from abc import ABC, abstractmethod
import os
import json
from filelock import FileLock


class HeaderRefreshService(ABC):
    """
    Abstract base class for header refresh services.
    
    Provides a common interface for refreshing headers from different API endpoints.
    Subclasses must implement the _fetch_new_headers method.
    """
    
    def __init__(self, new_headers: List[str], headers_url: str):
        """
        Initialize the header refresh service.
        
        Args:
            new_headers: List of header names to refresh
            headers_url: URL endpoint to fetch new headers from
        """
        if not new_headers or not isinstance(new_headers, list) or len(new_headers) == 0:
            raise ValueError("Headers list cannot be empty")
        
        if not headers_url or not headers_url.strip():
            raise ValueError("Headers URL cannot be empty")
        
        self.new_headers = new_headers
        self.headers_url = headers_url
        
        # Load credentials
        try:
            with open('config/credentials.yaml', 'r') as f:
                credentials = yaml.safe_load(f)
                self.token = credentials.get('token')
                if not self.token:
                    raise ValueError("Token not found in credentials.yaml")
        except Exception as e:
            logging.error(f"Failed to load credentials: {e}")
            raise
    
    @abstractmethod
    def _fetch_new_headers(self) -> List[Dict[str, Any]]:
        """
        Fetch new headers from the API endpoint.
        
        Returns:
            List[Dict[str, Any]]: List of new headers dictionaries
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        pass
    
    def _update_config_with_new_headers(self, new_headers: List[Dict[str, Any]]) -> bool:
        """
        Update configuration with new headers for all header types.
        
        Args:
            new_headers: List of new headers dictionaries
            
        Returns:
            bool: True if update was successful, False otherwise
        """
        try:
            success_count = 0
            
            for header_data in new_headers:
                headers_name = header_data.get('headers_name')
                headers = header_data.get('headers')
                
                if not headers_name or not headers:
                    logging.warning(f"Skipping invalid header data: {header_data}")
                    continue
                
                # Get current config for this header type
                current_config = get_header_config(headers_name)
                
                # Update the headers in the configuration
                updated_config = current_config.copy()
                updated_config.update(headers)
                
                # Update the header configuration
                success = update_header_config(headers_name, updated_config)
                
                if success:
                    logging.info(f"Successfully updated header configuration for '{headers_name}' with {len(headers)} items")
                    success_count += 1
                else:
                    logging.error(f"Failed to update header configuration for '{headers_name}'")
                    
            return success_count == len(new_headers)
            
        except Exception as e:
            logging.error(f"Error updating header configurations with new headers: {e}")
            raise Exception(f"Error updating header configurations with new headers: {e}")
    
    def refresh_headers_and_update_config(self) -> bool:
        """
        Main method to refresh headers and update configuration.
        
        Returns:
            bool: True if the entire process was successful, False otherwise
        """
        try:
            logging.info(f"Starting headers refresh process for headers: {self.new_headers}...")
            
            # Fetch new headers
            new_headers = self._fetch_new_headers()
            if not new_headers:
                logging.error(f"Failed to fetch new headers for {self.new_headers}")
                return False
            
            # Update configuration with all new headers at once
            success = self._update_config_with_new_headers(new_headers)
            if not success:
                logging.error(f"Failed to update header configurations for {self.new_headers}")
                return False
                
            logging.info(f"Successfully refreshed headers for: {[h.get('headers_name') for h in new_headers]}")
            return True
            
        except Exception as e:
            raise Exception(f'Error in headers refresh process: {e}')


class StandardHeaderRefreshService(HeaderRefreshService):
    """
    Standard implementation of header refresh service for most API endpoints.
    
    Uses standard HTTP GET requests with Bearer token authentication.
    """

    
    def _fetch_new_headers(self) -> List[Dict[str, Any]]:
        """
        Fetch new headers from the API endpoint using standard HTTP GET.
        
        Returns:
            List[Dict[str, Any]]: List of new headers dictionaries
        """
        try:
            headers = {
                'accept': 'application/json',
                'Authorization': f'Bearer {self.token}'
            }
            
            logging.info(f"Fetching new headers from: {self.headers_url}")
            response = requests.get(self.headers_url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                
                # Check for nested structure: credentials.headers
                if 'credentials' in data:
                    new_headers = [{'headers_name': c['api_type'], 'headers': c['headers']} for c in data['credentials']['credentials'] if c['api_type'] in self.new_headers]
                    logging.info(f"Successfully fetched {len(new_headers)} new headers from credentials.headers")
                    return new_headers
                else:
                    logging.error(f"Invalid response format: {data}")
                    return []
            else:
                logging.error(f"Failed to fetch headers. Status: {response.status_code}, Response: {response.text}")
                return []
                
        except Exception as e:
            logging.error(f"Error fetching headers: {e}")
            return []




# convenience functions
async def refresh_headers(new_headers: List[str], header_url: str):
    service = StandardHeaderRefreshService(new_headers, header_url)
    return service.refresh_headers_and_update_config()

