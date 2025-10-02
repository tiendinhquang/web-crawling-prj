import asyncio
import logging
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, Union
import httpx
import random
import os
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logging = logging.getLogger(__name__)


class ProxyError(Exception):
    """Custom exception for proxy operations."""
    pass


class BaseProxyProvider(ABC):
    """
    Abstract base class for defining a proxy provider.

    Any custom provider (e.g. file-based, API-based, etc.) must implement:
    - load_proxies(): Load a list of proxies.
    - check_proxy(): Check if a proxy is working (typically via a test request).
    """

    @abstractmethod
    def load_proxies(self, limit: int = 100) -> List[str]:
        """
        Load a list of proxies from the provider.
        
        Args:
            limit: Maximum number of proxies to return
            
        Returns:
            List[str]: List of proxy strings
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        pass
    
    @abstractmethod
    async def check_proxy(self, proxy: str) -> bool:
        """
        Check if a proxy is working.
        
        Args:
            proxy: Proxy string to test
            
        Returns:
            bool: True if proxy is working, False otherwise
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        pass
    
    
class FileProxyProvider(BaseProxyProvider):
    """
    Concrete implementation of BaseProxyProvider that loads proxies from a text file.

    Each line in the file should contain a proxy in the form: USER:PASSWORD@IP:PORT (without "http://").
    """

    def __init__(self, proxy_path: str):
        """
        Initialize the file-based proxy provider.
        
        Args:
            proxy_path: Path to the file containing proxy list
            
        Raises:
            ValueError: If proxy_path is empty or invalid
        """
        if not proxy_path or not proxy_path.strip():
            logging.error("Proxy path cannot be empty")
            raise ValueError("Proxy path cannot be empty")
        
        self.proxy_path = proxy_path
        logging.info(f"FileProxyProvider initialized with path: {proxy_path}")
        
    def load_proxies(self, limit: int = 100) -> List[str]:
        """
        Load and return a shuffled list of proxies from file.

        Args:
            limit: Maximum number of proxies to return

        Returns:
            List[str]: A list of proxies formatted as 'http://IP:PORT'
            
        Raises:
            ProxyError: If file cannot be read or parsed
            ValueError: If limit is invalid
        """
        if limit <= 0:
            logging.error(f"Invalid limit value: {limit}")
            raise ValueError(f"Limit must be positive, got: {limit}")
        
        if not os.path.exists(self.proxy_path):
            logging.error(f"Proxy file not found: {self.proxy_path}")
            raise ProxyError(f"Proxy file not found: {self.proxy_path}")
        
        try:
            with open(self.proxy_path, 'r') as f:
                lines = f.readlines()
                
            if not lines:
                logging.warning(f"Proxy file is empty: {self.proxy_path}")
                return []
            
            # Filter out empty lines and format proxies
            proxies = []
            for line in lines:
                line = line.strip()
                if line and not line.startswith('#'):  # Skip empty lines and comments
                    proxies.append(f'http://{line}')
            
            if not proxies:
                logging.warning(f"No valid proxies found in file: {self.proxy_path}")
                return []
            
            # Shuffle and limit
            random.shuffle(proxies)
            limited_proxies = proxies[:limit]
            
            logging.info(f"Loaded {len(limited_proxies)} proxies from {self.proxy_path}")
            return limited_proxies
            
        except PermissionError as e:
            logging.error(f"Permission denied reading proxy file {self.proxy_path}: {e}")
            raise ProxyError(f"Permission denied reading proxy file {self.proxy_path}: {e}")
        except UnicodeDecodeError as e:
            logging.error(f"Unicode decode error reading proxy file {self.proxy_path}: {e}")
            raise ProxyError(f"Unicode decode error reading proxy file {self.proxy_path}: {e}")
        except Exception as e:
            logging.error(f"Error loading proxies from {self.proxy_path}: {e}")
            raise ProxyError(f"Error loading proxies from {self.proxy_path}: {e}")
    
    async def check_proxy(self, proxy: str) -> bool:
        """
        Asynchronously test if a proxy works by sending a request to a test endpoint.

        Args:
            proxy: Proxy string (e.g. 'http://1.2.3.4:8080')

        Returns:
            bool: True if the proxy responds with status 200; False otherwise
        """
        if not proxy or not proxy.strip():
            logging.warning("Empty proxy string provided for testing")
            return False
        
        try:
            async with httpx.AsyncClient(
                proxies={'http://': proxy, 'https://': proxy},
                timeout=10.0  # 10 second timeout
            ) as client:
                logging.debug(f"Testing proxy: {proxy}")
                response = await client.get('https://api.ipify.org/?format=json')
                
                if response.status_code == 200:
                    logging.debug(f"Proxy {proxy} is working")
                    return True
                else:
                    logging.debug(f"Proxy {proxy} returned status {response.status_code}")
                    return False
                    
        except httpx.TimeoutException:
            logging.debug(f"Proxy {proxy} timed out")
            return False
        except httpx.ProxyError:
            logging.debug(f"Proxy {proxy} connection error")
            return False
        except httpx.RequestError as e:
            logging.debug(f"Proxy {proxy} request error: {e}")
            return False
        except Exception as e:
            logging.debug(f"Unexpected error testing proxy {proxy}: {e}")
            return False


class ProxyManager:
    """
    Manages proxy operations including loading and validation.
    
    Provides methods to load proxies from providers and validate their functionality.
    """
    
    def __init__(self, proxy_provider: BaseProxyProvider):
        """
        Initialize the proxy manager.
        
        Args:
            proxy_provider: Provider instance to load proxies from
            
        Raises:
            ValueError: If proxy_provider is None
        """
        if proxy_provider is None:
            logging.error("Proxy provider cannot be None")
            raise ValueError("Proxy provider cannot be None")
        
        self.proxy_provider = proxy_provider
        self.proxies = []
        logging.info("ProxyManager initialized successfully")
        
    async def get_working_proxies(self, limit: int = 100) -> List[str]:
        """
        Asynchronously load proxies and check which ones are working.

        Args:
            limit: Number of proxies to load and validate

        Returns:
            List[str]: List of proxies that passed the validation check
            
        Raises:
            ProxyError: If proxy loading fails
            ValueError: If limit is invalid
        """
        if limit <= 0:
            logging.error(f"Invalid limit value: {limit}")
            raise ValueError(f"Limit must be positive, got: {limit}")
        
        try:
            logging.info(f"Loading {limit} proxies for validation")
            raw_proxies = self.proxy_provider.load_proxies(limit)
            
            if not raw_proxies:
                logging.warning("No proxies loaded from provider")
                return []
            
            logging.info(f"Testing {len(raw_proxies)} proxies for functionality")
            tasks = [self.proxy_provider.check_proxy(proxy) for proxy in raw_proxies]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Handle exceptions in results
            working_proxies = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logging.warning(f"Exception testing proxy {raw_proxies[i]}: {result}")
                    continue
                elif result:  # result is True
                    working_proxies.append(raw_proxies[i])
            
            logging.info(f"Found {len(working_proxies)} working proxies out of {len(raw_proxies)} tested")
            return working_proxies
            
        except ProxyError:
            # Re-raise ProxyError from load_proxies
            raise
        except Exception as e:
            logging.error(f"Unexpected error getting working proxies: {e}")
            raise ProxyError(f"Unexpected error getting working proxies: {e}")
    
    def get_proxy_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the current proxy state.
        
        Returns:
            Dict[str, Any]: Statistics including total and working proxy counts
        """
        stats = {
            'total_proxies': len(self.proxies),
            'working_proxies': len([p for p in self.proxies if p]),  # Assuming working proxies are truthy
            'provider_type': type(self.proxy_provider).__name__
        }
        logging.debug(f"Proxy stats: {stats}")
        return stats


def get_working_proxies_sync(limit: int = 100, proxy_path: Optional[str] = None) -> Optional[List[str]]:
    """
    Synchronous wrapper to retrieve working proxies from an async context.

    Use this in synchronous environments (like Airflow DAGs, CLI scripts, etc.).

    Args:
        limit: Number of proxies to retrieve and validate
        proxy_path: Optional path to proxy file (defaults to 'config/proxies_config.txt')

    Returns:
        Optional[List[str]]: A list of valid proxies or None on failure
    """
    if limit <= 0:
        logging.error(f"Invalid limit value: {limit}")
        return None
    
    if proxy_path is None:
        proxy_path = 'config/proxies_config.txt'
    
    try:
        logging.info(f"Initializing proxy manager with path: {proxy_path}")
        proxy_provider = FileProxyProvider(proxy_path)
        proxy_manager = ProxyManager(proxy_provider)
        
        logging.info(f"Retrieving {limit} working proxies")
        working_proxies = asyncio.run(proxy_manager.get_working_proxies(limit))
        
        if working_proxies:
            logging.info(f"Successfully retrieved {len(working_proxies)} working proxies")
        else:
            logging.warning("No working proxies found")
        
        return working_proxies
        
    except ProxyError as e:
        logging.error(f"Proxy error: {e}")
        return None
    except ValueError as e:
        logging.error(f"Invalid configuration: {e}")
        return None
    except Exception as e:
        logging.error(f"Failed to get proxies: {e}")
        return None



        
