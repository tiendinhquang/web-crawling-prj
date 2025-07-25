
import httpx
import asyncio
import logging
import random
from typing import Dict, Any, Optional, Tuple, List
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum

# Import existing utils and error handler
from utils.common.config_manager import get_header_config, get_cookie_config
from utils.common.proxy_manager import get_working_proxies_sync
from services.error_handler import create_error_handler, UniversalErrorHandler


class SourceType(Enum):
    """Supported data sources"""
    WAYFAIR = "wayfair"
    WALMART = "walmart"
    SELLERCLOUD = "sellercloud"


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

@dataclass
class SourceConfig:
    """configuration for a specific data source"""
    source_type: SourceType
    api_url: str
    headers_name: str
    cookies_name: str
    api_type: Optional[str] = None  # For Wayfair: WayfairApiType value
    timeout_seconds: int = 30
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


class PayloadBuilder(ABC):
    """Abstract base class for payload builders"""
    @abstractmethod
    def build(self, **kwargs) -> Dict[str, Any]:
        """Build request payload based on API type and parameters"""
        pass


class WayfairPayloadBuilder(PayloadBuilder):
    """Wayfair-specific payload builder with different templates for each API type"""
    
    def __init__(self, api_type: WayfairApiType):
        self.api_type = api_type
    
    def build(self, **kwargs) -> Dict[str, Any]:
        """Build payload based on API type"""
        if self.api_type == WayfairApiType.PRODUCT_INFO:
            return self._build_product_info_payload(**kwargs)
        elif self.api_type == WayfairApiType.PRODUCT_DIMENSIONS:
            return self._build_product_dimensions_payload(**kwargs)
        elif self.api_type == WayfairApiType.PRODUCT_SPECIFICATION:
            return self._build_product_specification_payload(**kwargs)
        elif self.api_type == WayfairApiType.PRODUCT_REVIEWS:
            return self._build_product_reviews_payload(**kwargs)
        elif self.api_type == WayfairApiType.PRODUCT_DETAIL:
            return self._build_product_detail_payload(**kwargs)
        elif self.api_type == WayfairApiType.PRODUCT_LIST:
            return self._build_product_list_payload(**kwargs)
        else:
            raise ValueError(f"Unsupported Wayfair API type: {self.api_type}")
    
    def _build_product_info_payload(self, **kwargs) -> Dict[str, Any]:
        """Build payload for product info API"""
        sku = kwargs.get('sku', '')
        selected_options = kwargs.get('selected_options', [])
        
        return {
            'variables': {
                'sku': sku,
                'selectedOptionIds': selected_options,
                'energyLabelContext': 'PDPCAROUSEL',
            },
        }
    
    def _build_product_dimensions_payload(self, **kwargs) -> Dict[str, Any]:
        """Build payload for product dimensions API"""
        sku = kwargs.get('sku', '')
        selected_options = kwargs.get('selected_options', [])
        
        return {
            'operationName': 'Queries_Product_WeightsAndDimensions__',
            'variables': {
                'sku': sku,
            },
            'extensions': {
                'persistedQuery': {
                    'version': 1,
                    'sha256Hash': '8af395606197e405ce5734546f12159b28433db85d5faf0ae1a600afc92631be',
                },
            },
}

    
    def _build_product_specification_payload(self, **kwargs) -> Dict[str, Any]:
        """Build payload for product specification API"""
        sku = kwargs.get('sku', '')
        
        return {
            'operationName': 'specs',
            'variables': {
                'sku': sku,
            },
            'extensions': {
                'persistedQuery': {
                    'version': 1,
                    'sha256Hash': '731f41b9572fefb3f47cddc6ab143d198903c8475f753210b4fb044c89d912a4',
                },
            },
        }
    
    def _build_product_reviews_payload(self, **kwargs) -> Dict[str, Any]:
        """Build payload for product reviews API"""
        sku = kwargs.get('sku', '')
        page_number = kwargs.get('page_number', 1)
        reviews_per_page = kwargs.get('reviews_per_page', 10)
        
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
    
    def _build_product_detail_payload(self, **kwargs) -> Dict[str, Any]:
        """Build payload for product detail API"""
        sku = kwargs.get('sku', '')
        
        return {
            'sku': sku
        }
    
    def _build_product_list_payload(self, **kwargs) -> Dict[str, Any]:
        """Build payload for product list API"""
        return None

class WalmartPayloadBuilder(PayloadBuilder):
    """Walmart-specific payload builder"""
    def __init__(self, api_type: WalmartApiType):
        self.api_type = api_type
    
    def build(self, **kwargs) -> Dict[str, Any]:
        """Build payload for Walmart API"""
        if self.api_type == WalmartApiType.PRODUCT_LIST:
            return self._build_product_list_payload(**kwargs)
        else:   
            raise ValueError(f"Unsupported Walmart API type: {self.api_type}")
    
    def _build_product_list_payload(self, **kwargs) -> Dict[str, Any]:
        """Build payload for product list API"""
        return None

 

class BaseSourceClient(ABC):
    """
    Enhanced base client for all data sources.
    
    Integrates with existing error handler and utils for maximum code reuse.
    """
    
    def __init__(self, config: SourceConfig):
        self.config = config
        self.source_type = config.source_type
        
        # Initialize payload builder based on source and API type
        self.payload_builder = self._create_payload_builder()
        
        # Use existing error handler with source-specific configuration
        self.error_handler = create_error_handler(source=config.source_type.value)
    
    def _create_payload_builder(self) -> PayloadBuilder:
        """Create appropriate payload builder based on source and API type"""
        if self.config.source_type == SourceType.WAYFAIR:
            api_type = WayfairApiType(self.config.api_type) if self.config.api_type else WayfairApiType.PRODUCT_INFO
            return WayfairPayloadBuilder(api_type)
        elif self.config.source_type == SourceType.WALMART:
            api_type = WalmartApiType(self.config.api_type) if self.config.api_type else WalmartApiType.PRODUCT_LIST
            return WalmartPayloadBuilder(api_type)
        else:
            # For other sources, use default implementation
            raise ValueError(f"Unsupported source type: {self.config.source_type}")
    
    def build_request_payload(self, **kwargs) -> Dict[str, Any]:
        """Build request payload using the appropriate payload builder"""
        return self.payload_builder.build(**kwargs)
    
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

    def get_headers(self) -> Dict[str, str]:
        """Get headers using existing config manager"""
        return get_header_config(header_name=self.config.headers_name)
    
    def get_cookies(self) -> Dict[str, str]:
        """Get cookies using existing config manager"""
        return get_cookie_config(cookie_name=self.config.cookies_name)
    
    def get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers if needed"""
        headers = self.get_headers()
        if self.config.auth_token:
            headers['Authorization'] = f'Bearer {self.config.auth_token}'
        return headers
    
    async def _make_single_request(
        self, 
        url: str, 
        method: str = 'POST',
        payload: Optional[Dict] = None,
        params: Optional[Dict] = None,
        proxy: Optional[Dict] = None,
        **kwargs
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Make a single HTTP request with proper error handling using existing error handler.
        """
        headers = self.get_auth_headers()
        cookies = self.get_cookies()
        
        # Add source-specific parameters
        if self.config.api_hash:
            params = params or {}
            params['hash'] = self.config.api_hash
        
        async with httpx.AsyncClient(
            proxies=proxy,
            timeout=self.config.timeout_seconds,
            follow_redirects=True
        ) as client:
            if method.upper() == 'POST':
                response = await client.post(
                    url,
                    headers=headers,
                    params=params,
                    cookies=cookies,
                    json=payload
                )
            else:
                response = await client.get(
                    url,
                    headers=headers,
                    params=params,
                    cookies=cookies
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
                params=str(params) if params else '',
                **kwargs
            )
            
            return response_data, metadata
    
    async def make_request_with_retry(
        self,
        url: str,
        method: str = 'POST',
        payload: Optional[Dict] = None,
        params: Optional[Dict] = None,
        proxy: Optional[Dict] = None,
        semaphore: Optional[asyncio.Semaphore] = None,
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
                    self._make_single_request,
                    f"{self.build_metadata(url=url, method=method, payload=payload, params=params, proxy=proxy)}",
                    'make_request_with_retry',
                    url,
                    method,
                    payload,
                    params,
                    proxy,
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
        """
        if proxies is None:
            proxies = get_working_proxies_sync(limit=self.config.num_proxies)
        
        # Create semaphore if not provided to avoid event loop binding issues
        if semaphore is None:
            semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)
        
        tasks = []
        proxy_idx = 0
        
        for idx, item in enumerate(items):
            # Rotate proxies every 5 requests
            if proxies and idx % 5 == 0 and idx > 0:
                proxy_idx += 1
                logging.info(f"[🔄] Switching to proxy #{proxy_idx + 1} after {idx} requests")
            
            proxy = proxies[proxy_idx % len(proxies)] if proxies else None

    
            payload = self.build_request_payload(**item, **kwargs)
            
            tasks.append(
                self.make_request_with_retry(
      
                    payload=payload,
                    proxy=proxy,
                    semaphore=semaphore,
                    **item
                )
            )
        
        return await asyncio.gather(*tasks)


class DefaultPayloadBuilder(PayloadBuilder):
    """Default payload builder for sources without specific API types"""
    
    def build(self, **kwargs) -> Dict[str, Any]:
        """Default payload building logic"""
        item = kwargs.get('item', {})
        return item


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
    
    def build_metadata(self, **kwargs) -> Dict[str, Any]:
        """Build Wayfair-specific metadata"""
        metadata = {
            'source': 'wayfair',
            'url': kwargs.get('url', None),
            'proxy': kwargs.get('proxy', None),
            'payload': kwargs.get('payload', None),
            'params': kwargs.get('params', None), 
            'timestamp': asyncio.get_event_loop().time(),
        }
        
        # Add dynamic parameters that might be present in the request
        dynamic_params = ['sku', 'selected_options', 'keyword', 'page_number']
        for param in dynamic_params:
            if param in kwargs:
                metadata[param] = kwargs[param]
        
        return metadata


class WalmartClient(BaseSourceClient):
    """Enhanced Walmart-specific client implementation"""
    
    def parse_response(self, response: httpx.Response, **kwargs) -> Any:
        """Parse Walmart response"""
        try:
            return response.json()
        except Exception as e:
            raise ValueError(f"Failed to parse JSON: {e}")
    
    def validate_response(self, response_data: Dict[str, Any], **kwargs) -> bool:
        """Validate Walmart response"""
        return isinstance(response_data, dict)
    
    def build_metadata(self, **kwargs) -> Dict[str, Any]:
        """Extract Walmart-specific metadata"""
        metadata = {
            'source': 'walmart',
            'url': kwargs.get('url', None),
            'proxy': kwargs.get('proxy', None),
            'payload': kwargs.get('payload', None),
            'params': kwargs.get('params', None),
            'timestamp': asyncio.get_event_loop().time()
        }
        
        # Add dynamic parameters that might be present in the request
        dynamic_params = ['keyword', 'page_number', 'category', 'sort_by', 'filter_by']
        for param in dynamic_params:
            if param in kwargs:
                metadata[param] = kwargs[param]
        
        return metadata


class SellerCloudClient(BaseSourceClient):
    """Enhanced SellerCloud-specific client implementation"""
    
    def parse_response(self, response: httpx.Response, **kwargs) -> Any:
        """Parse SellerCloud response"""
        try:
            return response.json()
        except Exception as e:
            raise ValueError(f"Failed to parse JSON: {e}")
    
    def validate_response(self, response_data: Dict[str, Any], **kwargs) -> bool:
        """Validate SellerCloud response"""
        return 'product' in response_data or 'inventory' in response_data
    
    def build_metadata(self, **kwargs) -> Dict[str, Any]:
        """Extract SellerCloud-specific metadata"""
        item = kwargs.get('item', {})
        product_id = item.get('product_id', '')
        
        metadata = {
            'source': 'sellercloud',
            'product_id': product_id,
            'timestamp': asyncio.get_event_loop().time()
        }
        
        # Add dynamic parameters that might be present in the request
        dynamic_params = ['product_id', 'inventory_id', 'sku', 'keyword', 'page_number']
        for param in dynamic_params:
            if param in kwargs:
                metadata[param] = kwargs[param]
        
        return metadata


def create_source_client(source_type: SourceType, config: SourceConfig) -> BaseSourceClient:
    """Factory function to create enhanced source-specific client"""
    client_map = {
        SourceType.WAYFAIR: WayfairClient,
        SourceType.WALMART: WalmartClient,
        SourceType.SELLERCLOUD: SellerCloudClient,
    }
    
    if source_type not in client_map:
        raise ValueError(f"Unsupported source type: {source_type}")
    
    return client_map[source_type](config)
