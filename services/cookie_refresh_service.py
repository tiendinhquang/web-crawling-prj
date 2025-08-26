import requests
import logging
import yaml
from typing import Dict, Any, Optional
from utils.common.config_manager import get_cookie_config, update_cookie_config
from abc import ABC, abstractmethod


class CookieRefreshService(ABC):
    """
    Abstract base class for cookie refresh services.
    
    Provides a common interface for refreshing cookies from different API endpoints.
    Subclasses must implement the _fetch_new_cookies method.
    """
    
    def __init__(self, cookies_name: str, cookies_url: str):
        """
        Initialize the cookie refresh service.
        
        Args:
            cookies_name: Name identifier for the cookies in the config
            cookies_url: URL endpoint to fetch new cookies from
        """
        if not cookies_name or not cookies_name.strip():
            raise ValueError("Cookies name cannot be empty")
        
        if not cookies_url or not cookies_url.strip():
            raise ValueError("Cookies URL cannot be empty")
        
        self.cookies_name = cookies_name
        self.cookies_url = cookies_url
        
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
    def _fetch_new_cookies(self) -> Dict[str, Any]:
        """
        Fetch new cookies from the API endpoint.
        
        Returns:
            Dict[str, Any]: New cookies dictionary
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        pass
    
    def _update_config_with_new_cookies(self, new_cookies: Dict[str, Any]) -> bool:
        """
        Update configuration with new cookies.
        
        Args:
            new_cookies: New cookies dictionary
            
        Returns:
            bool: True if update was successful, False otherwise
        """
        try:
            current_config = get_cookie_config(self.cookies_name)
            
            # Update the cookies in the configuration
            updated_config = current_config.copy()
            updated_config.update(new_cookies)
            
            # Update the cookie configuration
            success = update_cookie_config(self.cookies_name, updated_config)
            
            if success:
                logging.info(f"Successfully updated cookie configuration for '{self.cookies_name}' with {len(new_cookies)} items")
            else:
                logging.error(f"Failed to update cookie configuration for '{self.cookies_name}'")
                
            return success
            
        except Exception as e:
            logging.error(f"Error updating cookie configuration for '{self.cookies_name}' with new cookies: {e}")
            return False
    
    def refresh_cookies_and_update_config(self) -> bool:
        """
        Main method to refresh cookies and update configuration.
        
        Returns:
            bool: True if the entire process was successful, False otherwise
        """
        try:
            logging.info(f"Starting cookies refresh process for '{self.cookies_name}'...")
            
            # Fetch new cookies
            new_cookies = self._fetch_new_cookies()
            if not new_cookies:
                logging.error(f"Failed to fetch new cookies for '{self.cookies_name}'")
                return False
            
            # Update configuration with new cookies
            success = self._update_config_with_new_cookies(new_cookies)
            
            if success:
                logging.info(f"Cookies refresh and configuration update completed successfully for '{self.cookies_name}'")
            else:
                logging.error(f"Cookies refresh completed but configuration update failed for '{self.cookies_name}'")
                
            return success
            
        except Exception as e:
            logging.error(f"Error in cookies refresh process for '{self.cookies_name}': {e}")
            return False


class StandardCookieRefreshService(CookieRefreshService):
    """
    Standard implementation of cookie refresh service for most API endpoints.
    
    Uses standard HTTP GET requests with Bearer token authentication.
    """
    
    def _fetch_new_cookies(self) -> Dict[str, Any]:
        """
        Fetch new cookies from the API endpoint using standard HTTP GET.
        
        Returns:
            Dict[str, Any]: New cookies dictionary
        """
        try:
            headers = {
                'accept': 'application/json',
                'Authorization': f'Bearer {self.token}'
            }
            
            logging.info(f"Fetching new cookies from: {self.cookies_url}")
            response = requests.get(self.cookies_url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                if 'cookies' in data:
                    new_cookies = data['cookies']
                    logging.info(f"Successfully fetched {len(new_cookies)} new cookies for '{self.cookies_name}'")
                    return new_cookies
                else:
                    logging.error(f"Invalid response format for '{self.cookies_name}': {data}")
                    return {}
            else:
                logging.error(f"Failed to fetch cookies for '{self.cookies_name}'. Status: {response.status_code}, Response: {response.text}")
                return {}
                
        except Exception as e:
            logging.error(f"Error fetching new cookies for '{self.cookies_name}': {e}")
            return {}


class LowesVendorCookieService(StandardCookieRefreshService):
    """
    Specialized cookie service for Lowes Vendor API.
    
    Inherits from StandardCookieRefreshService but can be extended with Lowes-specific logic.
    """
    
    def __init__(self):
        super().__init__(
            cookies_name='lowes_vendor',
            cookies_url='http://172.17.2.54:8000/api/v1/lowes/cookies'
        )


class WalmartAdCookieService(StandardCookieRefreshService):
    """
    Specialized cookie service for Walmart Ad API.
    
    Inherits from StandardCookieRefreshService but can be extended with Walmart-specific logic.
    """
    
    def __init__(self):
        super().__init__(
            cookies_name='walmart_ad',
            cookies_url='http://172.17.2.54:8000/api/v1/walmart/cookies'
        )

class GGMerchantsCookieService(StandardCookieRefreshService):
    """
    Specialized cookie service for GG Merchants API.
    
    Inherits from StandardCookieRefreshService but can be extended with GG Merchants-specific logic.
    """
    
    def __init__(self):
        super().__init__(
            cookies_name='gg_merchants',
            cookies_url='http://172.17.2.54:8000/api/v1/gg-merchants/cookies'
        )

class GGAdsCookieService(StandardCookieRefreshService):

    def __init__(self):
        super().__init__(
            cookies_name='gg_ads',
            cookies_url='http://172.17.2.54:8000/api/v1/gg-ads/cookies'
        )

# Factory function to create cookie services
def create_cookie_service(service_type: str) -> CookieRefreshService:
    """
    Factory function to create cookie refresh services.
    
    Args:
        service_type: Type of service ('lowes_vendor', 'walmart_ad', etc.)
        
    Returns:
        CookieRefreshService: Appropriate cookie service instance
        
    Raises:
        ValueError: If service_type is not supported
    """
    service_map = {
        'lowes_vendor': LowesVendorCookieService,
        'walmart_ad': WalmartAdCookieService,
        'gg_ads': GGAdsCookieService,
    }
    
    if service_type not in service_map:
        raise ValueError(f"Unsupported service type: {service_type}. Supported types: {list(service_map.keys())}")
    
    return service_map[service_type]()


# Convenience functions for backward compatibility
def refresh_lowes_vendor_cookies() -> bool:
    """Refresh cookies for Lowes Vendor service."""
    service = LowesVendorCookieService()
    return service.refresh_cookies_and_update_config()


def refresh_walmart_ad_cookies() -> bool:
    """Refresh cookies for Walmart Ad service."""
    service = WalmartAdCookieService()
    return service.refresh_cookies_and_update_config()

def refresh_gg_merchants_cookies() -> bool:
    """Refresh cookies for GG Merchants service."""
    service = GGMerchantsCookieService()
    return service.refresh_cookies_and_update_config()

def refresh_gg_ads_cookies() -> bool:
    """Refresh cookies for GG Ads service."""
    service = GGAdsCookieService()
    return service.refresh_cookies_and_update_config()

# if __name__ == "__main__":
#     # Example usage
#     try:
#         # Refresh Lowes cookies
#         print("Refreshing Lowes vendor cookies...")
#         success = refresh_lowes_vendor_cookies()
#         print(f"Lowes cookies refresh: {'Success' if success else 'Failed'}")
        
#         # Refresh Walmart cookies
#         print("Refreshing Walmart ad cookies...")
#         success = refresh_walmart_ad_cookies()
#         print(f"Walmart cookies refresh: {'Success' if success else 'Failed'}")
        
#     except Exception as e:
#         print(f"Error during cookie refresh: {e}") 