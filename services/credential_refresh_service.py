import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List
from utils.common.job_manager import JobManager

class BaseRefreshService(ABC):
    """
    Base class for refresh services (cookies, headers, etc.)
    Handles:
        - Credentials loading
        - Job creation
        - Polling job until done
        - Extracting result
    Subclasses implement:
        - _extract_data()
        - _update_config_with_new_data()
    """

    def __init__(self, create_job_url: str, payload: dict = {}):
        if not create_job_url or not create_job_url.strip():
            raise ValueError("create_job_url cannot be empty")

        self.create_job_url = create_job_url
        self.get_job_url = "http://172.17.1.205:8000/api/v1/jobs"
        self.payload = payload


    @abstractmethod
    def _extract_data(self, data: Dict[str, Any]):
        """Extract cookies/headers from job result"""
        pass

    @abstractmethod
    def _update_config_with_new_data(self, new_data) -> bool:
        """Write to config"""
        pass

    async def refresh_and_update_config(self) -> bool:
        logging.info(f"Refreshing using {self.__class__.__name__}...")
        try:
            job_id = JobManager().create_job(self.create_job_url, self.payload)
            data = JobManager().poll_job_until_done(job_id)
            new_data = self._extract_data(data)
            if not new_data:
                logging.error("No data extracted from job result")
                return False
            return self._update_config_with_new_data(new_data)
        except Exception as e:
            logging.error(f"Error in refresh process: {e}")
            return False



from utils.common.config_manager import get_cookie_config, update_cookie_config

class CookieRefreshService(BaseRefreshService):
    def __init__(self, cookies_name: str, create_job_url: str, payload: dict = {}):
        self.cookies_name = cookies_name
        super().__init__(create_job_url, payload)

    def _extract_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if "credentials" in data and "cookies" in data:
            return data["cookies"]
        return data

    def _update_config_with_new_data(self, new_cookies: Dict[str, Any]) -> bool:
        return update_cookie_config(self.cookies_name, new_cookies)



from utils.common.config_manager import get_header_config, update_header_config
from typing import List, Dict, Any

class HeaderRefreshService(BaseRefreshService):
    def __init__(self, new_headers: List[str], create_job_url: str, payload: dict = {}):
        self.new_headers = new_headers
        super().__init__(create_job_url, payload)

    def _extract_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        if "credentials" in data and isinstance(data["credentials"], list):
            return [
                {"headers_name": c["api_type"], "headers": c["headers"]}
                for c in data["credentials"]
                if f"headers:{c['api_type']}" in self.new_headers
            ]
        return [
            {"headers_name": new_header, "headers": data['headers']}
            for new_header in self.new_headers
        ]

    def _update_config_with_new_data(self, new_headers: List[Dict[str, Any]]) -> bool:

        success_count = 0
        for h in new_headers:
            if h['headers_name'].startswith("headers:"):
                h['headers_name'] = h['headers_name'].replace("headers:", "")
            if update_header_config(f"headers:{h['headers_name']}", h['headers']):
                success_count += 1
            else:
                logging.error(f"Failed to update header configuration for '{h['headers_name']}'")
        return success_count == len(new_headers)


class JobManagementService(BaseRefreshService):
    def __init__(self, create_job_url: str, payload: dict = {}):
        super().__init__(create_job_url, payload)

    def _extract_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return data

# ============================== Cookie Refresh Services ==============================





# wrapper function
async def refresh_lowes_vendor_cookies() -> bool:
    """Refresh cookies for Lowes Vendor service."""
    service = CookieRefreshService(cookies_name='cookies:lowes_vendor', create_job_url='http://172.17.1.205:8000/api/v1/lowes/crawl')
    return await service.refresh_and_update_config()


async def refresh_walmart_ad_cookies() -> bool:
    """Refresh cookies for Walmart Ad service."""
    service = CookieRefreshService(cookies_name='cookies:walmart_ad', create_job_url='http://172.17.1.205:8000/api/v1/walmart/crawl',
    payload={
        "vendor": "walmart_ad"
    }
    )

    return await service.refresh_and_update_config()

async def refresh_gg_merchants_cookies() -> bool:
    """Refresh cookies for GG Merchants service."""
    service = CookieRefreshService(cookies_name='cookies:gg_merchants', create_job_url='http://172.17.1.205:8000/api/v1/gg-merchants/crawl'
    )
    return await service.refresh_and_update_config()

async def refresh_walmart_seller_cookies() -> bool:
    """Refresh cookies for Walmart Seller service."""
    service = CookieRefreshService(cookies_name='cookies:walmart_seller', create_job_url='http://172.17.1.205:8000/api/v1/walmart/crawl',
    payload={
        "vendor": "walmart_seller"
    }
    )
    return await service.refresh_and_update_config()

async def refresh_wayfair_cookies(page_type: str = "wayfair_pdp") -> bool:
    """Refresh cookies for Wayfair service."""
    service = CookieRefreshService(cookies_name='cookies:wayfair_pdp', create_job_url='http://172.17.1.205:8000/api/v1/wayfair/crawl',
    payload={
        "page_type": page_type
    }
    )
    return await service.refresh_and_update_config()
async def refresh_gg_ads_cookies(force_new_session: bool = False) -> bool:
    service = CookieRefreshService(cookies_name='cookies:gg_ads', create_job_url='http://172.17.1.205:8000/api/v1/gg-ads/crawl',
    payload={
        "force_new_session": force_new_session
    }
    )
    return await service.refresh_and_update_config()


# wrapper function for header refresh
async def refresh_headers(new_headers: List[str], header_url: str, payload: dict = {}) -> bool:
    service = HeaderRefreshService(new_headers, header_url, payload)
    return await service.refresh_and_update_config()


