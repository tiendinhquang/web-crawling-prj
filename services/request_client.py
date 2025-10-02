
from calendar import c
import httpx
import asyncio
import logging
import random
from typing import Dict, Any, Optional, Tuple, List, Callable
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum

# Import existing utils and error handler
from utils.common.config_manager import get_header_config, get_cookie_config
from utils.common.proxy_manager import get_working_proxies_sync
from services.error_handler import create_error_handler, UniversalErrorHandler, ErrorContext


class SourceType(Enum):
    """Supported data sources"""
    WAYFAIR = "wayfair"
    WALMART = "walmart"
    CRITEO = "criteo"
    LOWES = "lowes"
    GG_MERCHANTS = "gg_merchants"
    GG_ADS = "gg_ads"

class WayfairApiType(Enum):
    """Wayfair API types"""
    PRODUCT_INFO = "product_info"
    PRODUCT_DIMENSIONS = "product_dimensions"
    PRODUCT_SPECIFICATION = "product_specification"
    PRODUCT_REVIEWS = "product_reviews"
    PRODUCT_DETAIL = "product_detail"
    PRODUCT_LIST = "product_list"

class WalmartApiType(Enum):
    """Walmart API types"""
    PRODUCT_LIST = "product_list"
    AD_REPORT = "ad_report"

class CriteoApiType(Enum):
    """Criteo API types"""
    CAPOUT = "capout"
    SEARCH_TERM = "search_term"
    PLACEMENT = "placement"
    BID = "bid"
    CAMPAIGN = "campaign"
    LINE_ITEM = "line_item"
class LowesApiType(Enum):
    """Lowes API types"""
    RTM = "rtm"
class GGMerchantsApiType(Enum):
    SKU_VISIBILITY = "sku_visibility"
class GGAdsApiType(Enum):
    AUCTION_INSIGHTS_DAILY_SEARCH = "auction_insights_daily_search"
@dataclass
class SourceConfig:
    """configuration for a specific data source"""
    source_type: SourceType
    api_url: str
    headers_name: str
    cookies_name: str
    api_type: Optional[str] = None  
    timeout_seconds: int = 60
    api_hash: Optional[str] = None
    auth_token: Optional[str] = None
    rate_limit_per_second: int = 2
    max_retries: int = 3
    retry_delay_seconds: int = 5
    batch_size: int = 50
    max_concurrent_requests: int = 10
    max_failures_per_batch: int = 10
    batch_delay_seconds: int = 20
    num_proxies: int = 30
    base_path: str = ''
    from_src: str = ''
    on_error: Optional[Callable[[ErrorContext], None]] = None


class BaseSourceClient(ABC):
    """
    Enhanced base client for all data sources.
    
    Integrates with existing error handler and utils for maximum code reuse.
    """
    
    def __init__(self, config: SourceConfig):
        self.config = config
        self.source_type = config.source_type
        
        # Use existing error handler with source-specific configuration
        self.error_handler = create_error_handler(source=config.source_type.value)
    
    @abstractmethod
    def validate_response(self, response_data: Dict[str, Any], **kwargs) -> bool:
        """Validate response data - must be implemented by subclasses"""
        pass
    
    @abstractmethod
    def build_metadata(self, **kwargs) -> Dict[str, Any]:
        """Extract metadata from response - must be implemented by subclasses"""
        pass

    @abstractmethod
    def parse_response(self, response: httpx.Response, **kwargs) -> Any:
        """Parse HTTP response into structured data (can be JSON, HTML, text, etc.)"""
        pass


    
    def get_cookies(self) -> Dict[str, str]:
        """Get cookies using existing config manager"""
        try:
            return get_cookie_config(name=self.config.cookies_name)
        except Exception as e:
            logging.warning(f"{e}")
            return {}
    
    def get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers if needed"""
        headers = get_header_config(name=self.config.headers_name)
        if self.config.auth_token:
            headers['Authorization'] = f'Bearer {self.config.auth_token}'
        return headers
    
    async def _make_single_request(
        self, 
        url: str, 
        method: str = 'POST',
        payload: Optional[Dict] = None,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
        proxy: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        cookies: Optional[Dict] = None,
        **kwargs
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Make a single HTTP request with proper error handling using existing error handler.
        """
        if headers is None:
            headers = self.get_auth_headers()
        if cookies is None:
            cookies = self.get_cookies()
        
        # Add source-specific parameters (like API hash) to params if provided
        if self.config.api_hash:
            params = params or {}
            params['hash'] = self.config.api_hash
            
        request_kwargs = {
            "headers": headers,
            **({"params": params} if params else {}),
            **({"cookies": cookies} if cookies else {}),
        }
        
        # Add json payload only for POST requests
        if method.upper() == 'POST' and payload:
            request_kwargs["json"] = payload
        if method.upper() == 'POST' and data:
            request_kwargs["data"] = data
        async with httpx.AsyncClient(
            proxies=proxy,
            timeout=self.config.timeout_seconds,
            follow_redirects=True
        ) as client:
            if method.upper() == 'POST':
                response = await client.post(
                    url,
                    **request_kwargs
                )
            else:
                response = await client.get(
                    url,
                    **request_kwargs
                )
            
            if not (200 <= response.status_code < 400):
                raise httpx.HTTPStatusError(
                    f"Non-success status code {response.status_code}",
                    request=response.request,
                    response=response
                )
            
            response_data = self.parse_response(response, **kwargs)
            
            # Validate response
            if not self.validate_response(response_data, **kwargs):
                raise ValueError("Response validation failed")
            
            # Extract metadata
            metadata = self.build_metadata(
                url=url,
                hash=self.config.api_hash,
                proxy=str(proxy) if proxy else '',
                payload=payload,  # Keep as dictionary
                data=data,
                params=str(params) if params else '',
                response_status_code=response.status_code,
                **kwargs
            )
            
            return response_data, metadata
    
    async def make_request_with_retry(
        self,
        url: str,
        method: str = 'POST',
        payload: Optional[Dict] = None,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
        proxy: Optional[Dict] = None,
        semaphore: Optional[asyncio.Semaphore] = None,
        headers: Optional[Dict] = None,
        cookies: Optional[Dict] = None,
        on_error: Optional[Callable[[ErrorContext], None]] = None,
        **kwargs
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Make HTTP request with retry logic using existing error handler.
        """
        # Rate limiting delay
        await asyncio.sleep(1 / self.config.rate_limit_per_second)
        
        if semaphore:
            async with semaphore:
                return await self.error_handler.execute_with_retry(
                    operation=self._make_single_request,
                    identifier=self.build_metadata(url=url, method=method, payload=payload, data=data, params=params, proxy=proxy),
                    operation_name='make_request_with_retry',
                    url=url,
                    method=method,
                    payload=payload,
                    data=data,
                    params=params,
                    proxy=proxy,
                    headers=headers,
                    cookies=cookies,
                    on_error=on_error,
                    **kwargs
                )
        else:
            raise ValueError("Semaphore is required for make_request_with_retry")

    
    async def process_batch(
        self,
        items: list,
        proxies: Optional[list] = None,
        semaphore: Optional[asyncio.Semaphore] = None,
        **kwargs
    ) -> list:
        """
        Process a batch of items with rotating proxies using existing proxy manager.
        Items should contain pre-built payload and/or params from DAG level.
        DAGs decide whether to use payload (JSON body) or params (URL parameters) based on API requirements.
        """
        if proxies is None:
            proxies = get_working_proxies_sync(limit=self.config.num_proxies)
        
        # Create semaphore if not provided to avoid event loop binding issues
        if semaphore is None:
            semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)
        # default handler
      
        tasks = []
        proxy_idx = 0
        
        for idx, item in enumerate(items):
            # Rotate proxies every 5 requests
            if proxies and idx % 5 == 0 and idx > 0:
                proxy_idx += 1
                logging.info(f"[🔄] Switching to proxy #{proxy_idx + 1} after {idx} requests")
            
            proxy = proxies[proxy_idx % len(proxies)] if proxies else None

            # Extract pre-built payload and params from item
            # DAGs can provide either payload (for POST requests) or params (for GET requests) or both
            payload = item.get('payload')
            params = item.get('params')
            url = item.get('url')
            method = item.get('method', 'POST')
            
            tasks.append(
                self.make_request_with_retry(
                    proxy=proxy,
                    semaphore=semaphore,
                    on_error=self.config.on_error,
                    **item
                )
            )
        
        return await asyncio.gather(*tasks)


class WayfairClient(BaseSourceClient):
    """Wayfair-specific client implementation"""

    def parse_response(self, response: httpx.Response, **kwargs) -> Any:
        """Parse Wayfair response"""
        try:
            return response.json()
        except Exception as e:
            raise ValueError(f"Failed to parse JSON: {e}")
    
    def validate_response(self, response_data: Dict[str, Any], **kwargs) -> bool:
        """Validate Wayfair response"""
        return isinstance(response_data, dict)
    def _build_reviews_payload(self, sku: str, page_number: int, reviews_per_page: int) -> Dict[str, Any]:
        """Build the reviews API request payload"""
        return {
            'variables': {
                'sku': sku,
                'sort_order': 'DATE_DESCENDING',
                'page_number': page_number,
                'filter_rating': '',
                'reviews_per_page': reviews_per_page,
                'search_query': '',
                'language_code': 'en',
            }
        }
    async def process_reviews_with_pagination(
        self,
        proxy: Optional[Dict] = None,
        semaphore: Optional[asyncio.Semaphore] = None,
        **kwargs
    ) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
        """
        Process reviews for a single SKU with pagination support.
        This is the specialized method for reviews that handles page-by-page processing.
        """
        # Don't create a new semaphore here - let the caller handle it
        # This avoids event loop binding issues
        if semaphore is None:
            logging.warning("No semaphore provided, creating a default one")
            semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)
        
        # Extract SKU and reviews_per_page from kwargs
        sku = kwargs.get('sku')
        if not sku:
            raise ValueError("SKU is required for reviews processing")
        
        # Use reviews_per_page from kwargs if provided, otherwise fall back to config default
        reviews_per_page = kwargs.get('reviews_per_page', 10000)
        
        results = []
        page = 1
        review_pages_total = None
        while True:
            # Build the request item for this page
            request_item = {
                'url': kwargs.get('url'),
                'method': 'POST',
                'payload': self._build_reviews_payload(sku, page, reviews_per_page),
                'params': kwargs.get('params'),
                'page_number': page,
                'reviews_per_page': reviews_per_page
            }
            
            try:
                # Make the request using the base client's process_batch method
                batch_results = await self.process_batch(
                    items=[request_item],
                    proxies=[proxy] if proxy else None,
                    semaphore=semaphore
                )
                
                if not batch_results or len(batch_results) == 0:
                    break
                
                # Extract the result
                result = batch_results[0]
                if result is None or result[1] is None:
                    break
                
                data, metadata = result
                
                # Add page number to the data for tracking
                data['page_number'] = page
                
                # Build metadata with SKU and other relevant information
                metadata = {
                    'sku': sku,
                    'page_number': page,
                    'reviews_per_page': reviews_per_page,
                    'url': kwargs.get('url'),
                    'params': kwargs.get('params')
                }
                
                # Return in the format expected by BaseSourceDAG: (data, metadata)
                results.append((data, metadata))
                
                # Check if we need to continue pagination
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
                    
            except Exception as e:
                logging.error(f"Failed to process page {page} for SKU {sku}: {e}")
                break
        
        return results
    def build_metadata(self, **kwargs) -> Dict[str, Any]:
        """Build Wayfair-specific metadata"""
        import time
        metadata = {
            'source': 'wayfair',
            'url': kwargs.get('url', None),
            'proxy': kwargs.get('proxy', None),
            'payload': kwargs.get('payload', None),
            'params': kwargs.get('params', None), 
            'timestamp': time.time(),
        }
        
        # Add dynamic parameters that might be present in the request
        dynamic_params = ['sku', 'selected_options', 'keyword', 'page_number', 'reviews_per_page']
        for param in dynamic_params:
            if param in kwargs:
                metadata[param] = kwargs[param]
        
        return metadata


class WalmartClient(BaseSourceClient):
    """Enhanced Walmart-specific client implementation"""
    
    def parse_response(self, response: httpx.Response, **kwargs) -> Any:
        """Parse Criteo response - accepts both JSON and text data"""
        try:
            # First try to parse as JSON
            return response.json()
        except Exception as json_error:
            try:
                # If JSON parsing fails, try to get text content
                text_content = response.content
                if text_content:
                    # Return text content as a dictionary with a 'text' key
                    return {'text': text_content, 'content_type': 'text'}
                else:
                    # If no text content, raise the original JSON error
                    raise ValueError(f"Failed to parse JSON: {json_error}")
            except Exception as text_error:
                # If both JSON and text parsing fail, raise the original JSON error
                raise ValueError(f"Failed to parse response as JSON or text: {json_error}")
    
    def validate_response(self, response_data: Dict[str, Any], **kwargs) -> bool:
        """Validate Walmart response"""
        return isinstance(response_data, dict)
    
    def build_metadata(self, **kwargs) -> Dict[str, Any]:
        """Extract Walmart-specific metadata"""
        import time
        metadata = {
            'source': 'walmart',
            'url': kwargs.get('url', None),
            'proxy': kwargs.get('proxy', None),
            'payload': kwargs.get('payload', None),
            'params': kwargs.get('params', None),
            'timestamp': time.time()
        }
        
        # Add dynamic parameters that might be present in the request
        dynamic_params = ['keyword', 'page_number', 'category', 'sort_by', 'filter_by']
        for param in dynamic_params:
            if param in kwargs:
                metadata[param] = kwargs[param]
        
        return metadata


class CriteoClient(BaseSourceClient):
    """Criteo-specific client implementation"""
    
    def parse_response(self, response: httpx.Response, **kwargs) -> Any:
        """Parse Criteo response - accepts both JSON and text data"""
        try:
            # First try to parse as JSON
            return response.json()
        except Exception as json_error:
            try:
                # If JSON parsing fails, try to get text content
                text_content = response.text
                if text_content:
                    # Return text content as a dictionary with a 'text' key
                    return {'text': text_content, 'content_type': 'text'}
                else:
                    # If no text content, raise the original JSON error
                    raise ValueError(f"Failed to parse JSON: {json_error}")
            except Exception as text_error:
                # If both JSON and text parsing fail, raise the original JSON error
                raise ValueError(f"Failed to parse response as JSON or text: {json_error}")
    
    def validate_response(self, response_data: Dict[str, Any], **kwargs) -> bool:
        """Validate Criteo response - accepts both dict and text responses"""
        # Accept both dictionary responses and text responses
        if isinstance(response_data, dict):
            return True
        elif isinstance(response_data, str):
            return True
        elif isinstance(response_data, dict) and 'text' in response_data:
            return True
        return False
    def build_metadata(self, **kwargs) -> Dict[str, Any]:
        """Build Criteo-specific metadata"""
        import time
        metadata = {
            'source': 'criteo',
            'url': kwargs.get('url', None),
            'proxy': kwargs.get('proxy', None),
            'params': kwargs.get('params', None),
            'campaign_id': kwargs.get('campaign_id', None),
            'line_item_id': kwargs.get('line_item_id', None),
            'period': kwargs.get('period', None),
            'dates': kwargs.get('dates', None),  
            'business_date': kwargs.get('business_date', None),
            'timestamp': time.time(),
            'response_status_code': kwargs.get('response_status_code', None),
        }
        return metadata


class LowesClient(BaseSourceClient):
    """Lowes-specific client implementation"""
    
    def parse_response(self, response: httpx.Response, **kwargs) -> Any:
        """Parse Lowes response"""
        try:
            return response.json()
        except Exception as e:
            raise ValueError(f"Failed to parse JSON: {e}")
    
    def validate_response(self, response_data: Dict[str, Any], **kwargs) -> bool:
        """Validate Lowes response"""
        return isinstance(response_data, dict)
    def build_metadata(self, **kwargs) -> Dict[str, Any]:
        """Build Lowes-specific metadata"""
        import time
        metadata = {
            'source': 'lowes',
            'url': kwargs.get('url', None),
            'proxy': kwargs.get('proxy', None),
            'params': kwargs.get('params', None),
            'payload': kwargs.get('payload', None),
            'tracking_number': kwargs.get('tracking_number', None),
            'business_date': kwargs.get('business_date', None),
            'timestamp': time.time()
        }
        return metadata

    


class GGMerchantsClient(BaseSourceClient):
    """GGMerchants-specific client implementation"""
    
    def parse_response(self, response: httpx.Response, **kwargs) -> Any:
        """Parse Criteo response - accepts both JSON and text data"""
        try:
            # First try to parse as JSON
            return response.json()
        except Exception as json_error:
            try:
                # If JSON parsing fails, try to get text content
                text_content = response.text
                if text_content:
                    # Return text content as a dictionary with a 'text' key
                    return {'text': text_content, 'content_type': 'text'}
                else:
                    # If no text content, raise the original JSON error
                    raise ValueError(f"Failed to parse JSON: {json_error}")
            except Exception as text_error:
                # If both JSON and text parsing fail, raise the original JSON error
                raise ValueError(f"Failed to parse response as JSON or text: {json_error}")
    def validate_response(self, response_data: Dict[str, Any], **kwargs) -> bool:
        """Validate Criteo response - accepts both dict and text responses"""
        # Accept both dictionary responses and text responses
        if isinstance(response_data, dict):
            return True
        elif isinstance(response_data, str):
            return True
        elif isinstance(response_data, dict) and 'text' in response_data:
            return True
        return False
    def build_metadata(self, **kwargs) -> Dict[str, Any]:
        """Build Criteo-specific metadata"""
        import time
        metadata = {
            'source': 'criteo',
            'url': kwargs.get('url', None),
            'proxy': kwargs.get('proxy', None),
            'payload': kwargs.get('payload', None),
            'data': kwargs.get('data', None),
            'params': kwargs.get('params', None),
            'response_status_code': kwargs.get('response_status_code', None),
            'timestamp': time.time(),
        }
        return metadata
class GGAdsClient(BaseSourceClient):
    """GGAds-specific client implementation"""
    
    def parse_response(self, response: httpx.Response, **kwargs) -> Any:
        """Parse Criteo response - accepts both JSON and text data"""
        try:
            # First try to parse as JSON
            return response.json()
        except Exception as json_error:
            try:
                # If JSON parsing fails, try to get text content
                text_content = response.text
                if text_content:
                    # Return text content as a dictionary with a 'text' key
                    return {'text': text_content, 'content_type': 'text'}
                else:
                    # If no text content, raise the original JSON error
                    raise ValueError(f"Failed to parse JSON: {json_error}")
            except Exception as text_error:
                # If both JSON and text parsing fail, raise the original JSON error
                raise ValueError(f"Failed to parse response as JSON or text: {json_error}")
    def validate_response(self, response_data: Dict[str, Any], **kwargs) -> bool:
        """Validate Criteo response - accepts both dict and text responses"""
        # Accept both dictionary responses and text responses
        if isinstance(response_data, dict):
            return True
        elif isinstance(response_data, str):
            return True
        elif isinstance(response_data, dict) and 'text' in response_data:
            return True
        return False
    def build_metadata(self, **kwargs) -> Dict[str, Any]:
        """Build Criteo-specific metadata"""
        import time
        metadata = {
            'source': 'criteo',
            'url': kwargs.get('url', None),
            'proxy': kwargs.get('proxy', None),
            'payload': kwargs.get('payload', None),
            'params': kwargs.get('params', None),
            'timestamp': time.time(),
        }
        return metadata

def create_source_client(source_type: SourceType, config: SourceConfig) -> BaseSourceClient:
    """Factory function to create enhanced source-specific client"""
    client_map = {
        SourceType.WAYFAIR: WayfairClient,
        SourceType.WALMART: WalmartClient,
        SourceType.CRITEO: CriteoClient,
        SourceType.LOWES: LowesClient,
        SourceType.GG_MERCHANTS: GGMerchantsClient,
        SourceType.GG_ADS: GGAdsClient
    }
    
    if source_type not in client_map:
        raise ValueError(f"Unsupported source type: {source_type}")
    
    return client_map[source_type](config)

