import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union
from utils.common.sharepoint.auth import AzureAuth
import requests
import yaml
import httpx
import os
from pathlib import Path
import asyncio

# Configure logging
logging.basicConfig(level=logging.INFO)


# Load configuration with error handling
try:
    with open('config/azure.yaml', 'r') as f:
        data = yaml.safe_load(f)
    SHAREPOINT_HOST = data['SHAREPOINT_HOST']
except FileNotFoundError:
    logging.error("Configuration file 'config/azure.yaml' not found")
    raise
except KeyError as e:
    logging.error(f"Missing required configuration key: {e}")
    raise
except yaml.YAMLError as e:
    logging.error(f"Error parsing YAML configuration: {e}")
    raise

BASE_URL = 'https://graph.microsoft.com/v1.0'


class SharePointError(Exception):
    """Custom exception for SharePoint operations."""
    pass


class BaseSharepointManager(ABC):
    """
    Abstract base class for SharePoint managers.
    
    Provides common functionality for SharePoint operations including
    authentication and basic site operations.
    """
    
    def __init__(self):
        """
        Initialize the SharePoint manager with authentication.
        
        Raises:
            SharePointError: If authentication fails
        """
        try:
            self.auth = AzureAuth()
            auth_response = self.auth.get_access_token()
            self.access_token = auth_response['access_token']
            logging.info("SharePoint authentication successful")
        except Exception as e:
            logging.error(f"Failed to authenticate with SharePoint: {e}")
            raise SharePointError(f"Authentication failed: {e}")
    
    @abstractmethod
    def get_site_id(self, site_name: str) -> str:
        """
        Get the site ID for a given site name.
        
        Args:
            site_name: Name of the SharePoint site
            
        Returns:
            str: Site ID
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        pass
    
    @abstractmethod
    def get_drive_id(self, site_id: str, drive_name: str) -> Optional[str]:
        """
        Get the drive ID for a given drive name in a site.
        
        Args:
            site_id: SharePoint site ID
            drive_name: Name of the drive
            
        Returns:
            Optional[str]: Drive ID if found, None otherwise
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        pass
    
    @abstractmethod
    def get_item_id(self, site_id: str, drive_id: str, folder_path: str, file_name: str) -> Optional[str]:
        """
        Get the item ID for a given file in a folder.
        
        Args:
            site_id: SharePoint site ID
            drive_id: Drive ID
            folder_path: Path to the folder
            file_name: Name of the file
            
        Returns:
            Optional[str]: Item ID if found, None otherwise
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        pass


class SharepointManager(BaseSharepointManager):
    """
    Concrete implementation of SharePoint manager for site and file operations.
    
    Provides methods to interact with SharePoint sites, drives, and files
    through Microsoft Graph API.
    """
    
    def __init__(self):
        """
        Initialize the SharePoint manager.
        
        Raises:
            SharePointError: If authentication fails
        """
        super().__init__()
        logging.info("SharePoint manager initialized successfully")
    
    def get_site_id(self, site_name: str) -> str:
        """
        Get the site ID for a given site name.
        
        Args:
            site_name: Name of the SharePoint site
            
        Returns:
            str: Site ID
            
        Raises:
            SharePointError: If the site is not found or API call fails
            ValueError: If site_name is empty or invalid
        """
        if not site_name or not site_name.strip():
            logging.error("Site name cannot be empty")
            raise ValueError("Site name cannot be empty")
        
        try:
            url = f"{BASE_URL}/sites/{SHAREPOINT_HOST}:/sites/{site_name}"
            headers = {"Authorization": f"Bearer {self.access_token}"}
            
            logging.info(f"Fetching site ID for site: {site_name}")
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            site_data = response.json()
            site_id = site_data.get('id')
            
            if not site_id:
                logging.error(f"Site ID not found in response for site: {site_name}")
                raise SharePointError(f"Site ID not found for site: {site_name}")
            
            logging.info(f"Successfully retrieved site ID: {site_id} for site: {site_name}")
            return site_id
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to get site ID for {site_name}: {e}")
            raise SharePointError(f"Failed to get site ID for {site_name}: {e}")
        except KeyError as e:
            logging.error(f"Unexpected response format for site {site_name}: {e}")
            raise SharePointError(f"Unexpected response format for site {site_name}")
    
    def get_all_drives_in_site(self, site_id: str) -> List[Dict[str, Any]]:
        """
        Get all drives in a SharePoint site.
        
        Args:
            site_id: SharePoint site ID
            
        Returns:
            List[Dict[str, Any]]: List of drive information dictionaries
            
        Raises:
            SharePointError: If API call fails
            ValueError: If site_id is empty or invalid
        """
        if not site_id or not site_id.strip():
            logging.error("Site ID cannot be empty")
            raise ValueError("Site ID cannot be empty")
        
        try:
            url = f"{BASE_URL}/sites/{site_id}/drives"
            headers = {"Authorization": f"Bearer {self.access_token}"}
            
            logging.info(f"Fetching all drives for site ID: {site_id}")
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            drives_data = response.json()
            drives = drives_data.get('value', [])
            
            formatted_drives = [
                {
                    'id': drive['id'],
                    'name': drive['name'],
                    'web_url': drive['webUrl'],
                    'last_modified_time': drive['lastModifiedDateTime'],
                } for drive in drives
            ]
            
            logging.info(f"Successfully retrieved {len(formatted_drives)} drives for site ID: {site_id}")
            return formatted_drives
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to get drives for site ID {site_id}: {e}")
            raise SharePointError(f"Failed to get drives for site ID {site_id}: {e}")
        except KeyError as e:
            logging.error(f"Unexpected response format for drives in site {site_id}: {e}")
            raise SharePointError(f"Unexpected response format for drives in site {site_id}")
    
    def get_drive_id(self, site_id: str, drive_name: str) -> Optional[str]:
        """
        Get the drive ID for a given drive name in a site.
        
        Args:
            site_id: SharePoint site ID
            drive_name: Name of the drive
            
        Returns:
            Optional[str]: Drive ID if found, None otherwise
            
        Raises:
            SharePointError: If API call fails
            ValueError: If site_id or drive_name is empty or invalid
        """
        if not site_id or not site_id.strip():
            logging.error("Site ID cannot be empty")
            raise ValueError("Site ID cannot be empty")
        
        if not drive_name or not drive_name.strip():
            logging.error("Drive name cannot be empty")
            raise ValueError("Drive name cannot be empty")
        
        try:
            drives = self.get_all_drives_in_site(site_id)
            
            for drive in drives:
                if drive['name'] == drive_name:
                    logging.info(f"Found drive ID: {drive['id']} for drive name: {drive_name}")
                    return drive['id']
            
            logging.warning(f"Drive '{drive_name}' not found in site ID: {site_id}")
            return None
            
        except SharePointError:
            # Re-raise SharePointError from get_all_drives_in_site
            raise
        except Exception as e:
            logging.error(f"Unexpected error getting drive ID for {drive_name}: {e}")
            raise SharePointError(f"Unexpected error getting drive ID for {drive_name}: {e}")
    
    def get_items_in_folder(self, site_id: str, drive_id: str, folder_path: str) -> Dict[str, Any]:
        """
        Get all items in a SharePoint folder.
        
        Args:
            site_id: SharePoint site ID
            drive_id: Drive ID
            folder_path: Path to the folder (e.g., 'Web Crawling/Wayfair')
            
        Returns:
            Dict[str, Any]: JSON response containing folder items
            
        Raises:
            SharePointError: If API call fails
            ValueError: If any parameter is empty or invalid
        """
        if not site_id or not site_id.strip():
            logging.error("Site ID cannot be empty")
            raise ValueError("Site ID cannot be empty")
        
        if not drive_id or not drive_id.strip():
            logging.error("Drive ID cannot be empty")
            raise ValueError("Drive ID cannot be empty")
        
        if not folder_path or not folder_path.strip():
            logging.error("Folder path cannot be empty")
            raise ValueError("Folder path cannot be empty")
        
        try:
            url = f"{BASE_URL}/sites/{site_id}/drives/{drive_id}/root:/{folder_path.rstrip('/')}:/children"
            headers = {"Authorization": f"Bearer {self.access_token}"}
            
            logging.info(f"Fetching items in folder: {folder_path}")
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            items_data = response.json()
            logging.info(f"Successfully retrieved {len(items_data.get('value', []))} items from folder: {folder_path}")
            return items_data
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to get items in folder {folder_path}: {e}")
            raise SharePointError(f"Failed to get items in folder {folder_path}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error getting items in folder {folder_path}: {e}")
            raise SharePointError(f"Unexpected error getting items in folder {folder_path}: {e}")
    
    def get_item_id(self, site_id: str, drive_id: str, folder_path: str, file_name: str) -> Optional[str]:
        """
        Get the item ID for a given file in a folder.
        
        Args:
            site_id: SharePoint site ID
            drive_id: Drive ID
            folder_path: Path to the folder
            file_name: Name of the file
            
        Returns:
            Optional[str]: Item ID if found, None otherwise
            
        Raises:
            SharePointError: If API call fails
            ValueError: If any parameter is empty or invalid
        """
        if not site_id or not site_id.strip():
            logging.error("Site ID cannot be empty")
            raise ValueError("Site ID cannot be empty")
        
        if not drive_id or not drive_id.strip():
            logging.error("Drive ID cannot be empty")
            raise ValueError("Drive ID cannot be empty")
        
        if not folder_path or not folder_path.strip():
            logging.error("Folder path cannot be empty")
            raise ValueError("Folder path cannot be empty")
        
        if not file_name or not file_name.strip():
            logging.error("File name cannot be empty")
            raise ValueError("File name cannot be empty")
        
        try:
            items = self.get_items_in_folder(site_id, drive_id, folder_path)
            
            for item in items.get('value', []):
                if item.get('name') == file_name:
                    item_id = item.get('id')
                    logging.info(f"Found item ID: {item_id} for file: {file_name}")
                    return item_id
            
            logging.warning(f"File '{file_name}' not found in folder: {folder_path}")
            return None
            
        except SharePointError:
            # Re-raise SharePointError from get_items_in_folder
            raise
        except Exception as e:
            logging.error(f"Unexpected error getting item ID for {file_name}: {e}")
            raise SharePointError(f"Unexpected error getting item ID for {file_name}: {e}")
    
    async def upload_file(self, client: httpx.AsyncClient, site_id: str, drive_id: str, 
                         local_path: str, remote_path: str) -> Dict[str, Any]:
        """
        Upload a file to SharePoint.
        
        Args:
            client: HTTPX async client
            site_id: SharePoint site ID
            drive_id: Drive ID
            local_path: Local file path
            remote_path: Remote file path in SharePoint
            
        Returns:
            Dict[str, Any]: Upload response data
            
        Raises:
            SharePointError: If upload fails
            FileNotFoundError: If local file doesn't exist
            ValueError: If any parameter is empty or invalid
        """
        if not site_id or not site_id.strip():
            logging.error("Site ID cannot be empty")
            raise ValueError("Site ID cannot be empty")
        
        if not drive_id or not drive_id.strip():
            logging.error("Drive ID cannot be empty")
            raise ValueError("Drive ID cannot be empty")
        
        if not local_path or not local_path.strip():
            logging.error("Local path cannot be empty")
            raise ValueError("Local path cannot be empty")
        
        if not remote_path or not remote_path.strip():
            logging.error("Remote path cannot be empty")
            raise ValueError("Remote path cannot be empty")
        
        # Check if local file exists
        if not os.path.exists(local_path):
            logging.error(f"Local file not found: {local_path}")
            raise FileNotFoundError(f"Local file not found: {local_path}")
        
        try:
            import aiofiles
            url = f"{BASE_URL}/sites/{site_id}/drives/{drive_id}/root:/{remote_path}:/content"
            headers = {"Authorization": f"Bearer {self.access_token}"}
            
            logging.info(f"Uploading file from {local_path} to {remote_path}")
            
            async with aiofiles.open(local_path, "rb") as f:
                file_content = await f.read()
                response = await client.put(url, headers=headers, content=file_content)
                response.raise_for_status()
            
            upload_data = response.json()
            logging.info(f"Successfully uploaded file to {remote_path}")
            return upload_data
            
        except ImportError:
            logging.error("aiofiles module is required for file upload")
            raise SharePointError("aiofiles module is required for file upload")
        except httpx.HTTPStatusError as e:
            logging.error(f"HTTP error during file upload: {e}")
            raise SharePointError(f"HTTP error during file upload: {e}")
        except Exception as e:
            logging.error(f"Unexpected error during file upload: {e}")
            raise SharePointError(f"Unexpected error during file upload: {e}")
    async def upload_large_file(
        self,
        client: httpx.AsyncClient,
        chunk_size: int, # will be MB
        site_id: str,
        drive_id: str,
        remote_path: str,
        local_path: str,
        retries_attempt: int = 3
    ):
        """
        Upload a large file to SharePoint. The file is considered large if it is larger than 256MB.
        Args:
            client: HTTPX async client
            chunk_size: Size of each chunk to upload
            site_id: SharePoint site ID
            drive_id: Drive ID
            remote_path: Remote file path in SharePoint
            local_path: Local file path
            retries_attempt: Number of times to retry the upload
            
        Returns:
            Dict[str, Any]: Upload response data
            
        """
        import aiofiles
        # Create upload session
        url = f"{BASE_URL}/sites/{site_id}/drives/{drive_id}/root:/{remote_path}:/createUploadSession"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        session_res = await client.post(url, headers=headers)
        session_res.raise_for_status()
        upload_url = session_res.json()['uploadUrl']
        if not site_id or not site_id.strip():
            logging.error("Site ID cannot be empty")
            raise ValueError("Site ID cannot be empty")
        if not drive_id or not drive_id.strip():
            logging.error("Drive ID cannot be empty")
            raise ValueError("Drive ID cannot be empty")
        if not remote_path or not remote_path.strip():
            logging.error("Remote path cannot be empty")
            raise ValueError("Remote path cannot be empty")
        if not local_path or not local_path.strip():
            logging.error("Local path cannot be empty")
            raise ValueError("Local path cannot be empty")
        if not retries_attempt or retries_attempt <= 0:
            logging.error("Retries attempt must be greater than 0")
        if not chunk_size: # cannot large than 256MB
            chunk_size = 10 * 1024 * 1024  # 10MB
            logging.info(f"Chunk size not provided, using default: {chunk_size} bytes")
        elif chunk_size > 256:
            logging.error("Chunk size cannot be larger than 256MB")
            raise ValueError("Chunk size cannot be larger than 256MB")
        chunk_size = chunk_size * 1024 * 1024 # convert to bytes
        total_size = os.path.getsize(local_path)
        uploaded = 0
        logging.info(f"Uploading file '{local_path}' ({total_size} bytes) to '{remote_path}'")

        async with aiofiles.open(local_path, 'rb') as f:
            while uploaded < total_size:
                chunk = await f.read(chunk_size)
                start = uploaded
                end = uploaded + len(chunk) - 1
                content_range = f"bytes {start}-{end}/{total_size}"

                for attempt in range(1, retries_attempt + 1):
                    try:
                        resp = await client.put(
                            upload_url,
                            headers={
                                "Content-Length": str(len(chunk)),
                                "Content-Range": content_range,
                            },
                            content=chunk,
                        )

                        if resp.status_code in [200, 201]:
                            logging.info(f"✅ Upload complete: {remote_path}")
                            return resp.json()
                        elif resp.status_code == 202:
                            logging.info(f"🟢 Chunk uploaded: {content_range}")
                            uploaded += len(chunk)
                            break
                        else:
                            logging.warning(f"⚠️ Chunk upload failed (attempt {attempt}/{retries_attempt}) "
                                            f"with status {resp.status_code}: {resp.text}")
                    except httpx.RequestError as e:
                        logging.warning(f"🔁 Network error on attempt {attempt}/{retries_attempt}: {e}")

                    if attempt < retries_attempt:
                        wait_time = 2 ** attempt
                        logging.info(f"⏳ Retrying in {wait_time}s...")
                        await asyncio.sleep(wait_time)
                    else:
                        raise Exception(f"❌ Failed to upload chunk {content_range} after {retries_attempt} attempts.")

        raise Exception("Upload session finished without success (no 200/201 response).")



class BaseExcelOnlineLoader(ABC):
    """
    Abstract base class for Excel Online operations.
    
    Provides common functionality for working with Excel files
    through Microsoft Graph API.
    """
    
    def __init__(self):
        """
        Initialize the Excel Online loader with authentication.
        
        Raises:
            SharePointError: If authentication fails
        """
        try:
            self.auth = AzureAuth()
            auth_response = self.auth.get_access_token()
            self.access_token = auth_response['access_token']
            logging.info("Excel Online authentication successful")
        except Exception as e:
            logging.error(f"Failed to authenticate for Excel Online: {e}")
            raise SharePointError(f"Excel Online authentication failed: {e}")
    
    def _get_workbook_session_id(self, site_id: str, item_id: str) -> str:
        """
        Get a workbook session ID for Excel operations.
        
        Args:
            site_id: SharePoint site ID
            item_id: Excel file item ID
            
        Returns:
            str: Workbook session ID
            
        Raises:
            SharePointError: If session creation fails
            ValueError: If site_id or item_id is empty or invalid
        """
        if not site_id or not site_id.strip():
            logging.error("Site ID cannot be empty")
            raise ValueError("Site ID cannot be empty")
        
        if not item_id or not item_id.strip():
            logging.error("Item ID cannot be empty")
            raise ValueError("Item ID cannot be empty")
        
        try:
            url = f"{BASE_URL}/sites/{site_id}/drive/items/{item_id}/workbook/createSession"
            headers = {"Authorization": f"Bearer {self.access_token}"}
            
            logging.info(f"Creating workbook session for item ID: {item_id}")
            response = requests.post(url, headers=headers)
            response.raise_for_status()
            
            session_data = response.json()
            session_id = session_data.get('id')
            
            if not session_id:
                logging.error(f"Session ID not found in response for item: {item_id}")
                raise SharePointError(f"Session ID not found for item: {item_id}")
            
            logging.info(f"Successfully created workbook session: {session_id}")
            return session_id
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to create workbook session for item {item_id}: {e}")
            raise SharePointError(f"Failed to create workbook session for item {item_id}: {e}")
        except KeyError as e:
            logging.error(f"Unexpected response format for workbook session: {e}")
            raise SharePointError(f"Unexpected response format for workbook session")
        
    @abstractmethod
    def get_used_range(self, site_id: str, drive_name: str, file_path: str, 
                      sheet_name: str, range_address: str) -> Dict[str, Any]:
        """
        Get the used range from an Excel worksheet.
        
        Args:
            site_id: SharePoint site ID
            drive_name: Name of the drive
            file_path: Path to the Excel file
            sheet_name: Name of the worksheet
            range_address: Excel range address (e.g., 'A1:D10')
            
        Returns:
            Dict[str, Any]: Excel range data
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        pass
    def update_cell(self, site_id: str, drive_name: str, file_path: str, sheet_name: str, range_address: str, value: dict) -> Dict[str, Any]:
        """
        Update a cell in an Excel worksheet.
        
        Args:
            site_id: SharePoint site ID
            drive_name: Name of the drive
            file_path: Path to the Excel file
            sheet_name: Name of the worksheet
            range_address: Excel range address (e.g., 'A1:D10')
            value: Value to update the cell with
            
        """
        pass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# assume these are imported from your codebase
# from .sharepoint_manager import SharepointManager
# from .exceptions import SharePointError
# from .base_loader import BaseExcelOnlineLoader

BASE_URL = "https://graph.microsoft.com/v1.0"

DEFAULT_RETRY_STRATEGY = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "PATCH", "POST", "PUT", "DELETE", "HEAD"])
)


class ExcelOnlineLoader(BaseExcelOnlineLoader):
    """
    Cleaner Excel Online loader:
    - centralizes validation
    - reuses requests.Session with retry
    - extracts common helpers (parse path, get ids, build headers, request wrapper)
    - easier to unit-test (deps can be injected)
    """

    def __init__(
        self,
        sharepoint_manager: Optional[Any] = None,
        session: Optional[requests.Session] = None,
        retry_strategy: Optional[Retry] = None,
    ):
        super().__init__()
        self._sp = sharepoint_manager or SharepointManager()
        self.session = session or self._create_session(retry_strategy or DEFAULT_RETRY_STRATEGY)
        logging.info("Excel Online loader initialized successfully")

    @staticmethod
    def _create_session(retry_strategy: Retry) -> requests.Session:
        s = requests.Session()
        adapter = HTTPAdapter(max_retries=retry_strategy)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        return s

    @staticmethod
    def _validate_non_empty(**kwargs) -> None:
        """Generic validator: raises ValueError if any value is empty/whitespace."""
        for name, val in kwargs.items():
            if val is None:
                logging.error("%s cannot be None", name)
                raise ValueError(f"{name} cannot be None")
            if isinstance(val, str) and not val.strip():
                logging.error("%s cannot be empty", name)
                raise ValueError(f"{name} cannot be empty")

    @staticmethod
    def _parse_file_path(file_path: str) -> Tuple[str, str]:
        """
        Returns (file_name, folder_path).
        folder_path is normalized with forward slashes and does not end with a slash (unless root).
        """
        p = Path(file_path)
        file_name = p.name
        parent = p.parent.as_posix()
        if parent == ".":
            parent = ""
        return file_name, parent

    def _get_drive_and_item(self, site_id: str, drive_name: str, folder_path: str, file_name: str) -> Tuple[str, str]:
        drive_id = self._sp.get_drive_id(site_id, drive_name)
        if not drive_id:
            logging.error("Drive '%s' not found in site ID: %s", drive_name, site_id)
            raise SharePointError(f"Drive '{drive_name}' not found in site ID: {site_id}")

        item_id = self._sp.get_item_id(site_id, drive_id, folder_path, file_name)
        if not item_id:
            logging.error("File '%s' not found in folder: %s", file_name, folder_path)
            raise SharePointError(f"File '{file_name}' not found in folder: {folder_path}")

        return drive_id, item_id

    def _build_headers(self, session_id: Optional[str] = None) -> Dict[str, str]:
        headers = {"Authorization": f"Bearer {self.access_token}"}
        if session_id:
            headers["Workbook-Session-Id"] = session_id
        return headers

    def _request(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        """
        Central request wrapper: handles logging, status check, and unified exception mapping.
        """
        try:
            logging.debug("Request %s %s (kwargs keys: %s)", method, url, list(kwargs.keys()))
            resp = self.session.request(method, url, timeout=30, **kwargs)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            logging.error("HTTP request failed: %s %s - %s", method, url, e)
            raise SharePointError(f"HTTP request failed: {e}")
        except ValueError as e:
            # JSON decode error
            logging.error("Failed to decode JSON response from %s %s: %s", method, url, e)
            raise SharePointError(f"Failed to decode JSON response: {e}")

    def _get_workbook_session_id(self, site_id: str, item_id: str) -> Optional[str]:
        # reuse your existing method (keeps name compatibility). If it returns None, we continue without session.
        return super()._get_workbook_session_id(site_id, item_id)

    # Public API ----------------------------------------------------

    def get_used_range(
        self,
        site_id: str,
        drive_name: str,
        file_path: str,
        sheet_name: str,
        range_address: Optional[str] = None,
    ) -> Dict[str, Any]:
        self._validate_non_empty(site_id=site_id, drive_name=drive_name, file_path=file_path, sheet_name=sheet_name)

        file_name, folder_path = self._parse_file_path(file_path)

        try:
            _, item_id = self._get_drive_and_item(site_id, drive_name, folder_path, file_name)
            session_id = self._get_workbook_session_id(site_id, item_id)
            if range_address:
                # explicit range then usedRange of that range
                url = f"{BASE_URL}/sites/{site_id}/drive/items/{item_id}/workbook/worksheets/{sheet_name}/range(address='{range_address}')/usedRange"
            else:
                url = f"{BASE_URL}/sites/{site_id}/drive/items/{item_id}/workbook/worksheets/{sheet_name}/range/usedRange"

            headers = self._build_headers(session_id)
            logging.info("Fetching used range '%s' from sheet '%s' in file '%s'", range_address or "all", sheet_name, file_name)
            return self._request("GET", url, headers=headers)

        except SharePointError:
            raise
        except Exception as e:
            logging.exception("Unexpected error getting used range")
            raise SharePointError(f"Unexpected error getting used range: {e}")
        
    def col_index_to_name(self, index: int) -> str:
        import string
        result = []
        while index > 0:
            index, remainder = divmod(index - 1, 26)
            result.append(string.ascii_uppercase[remainder])
        return ''.join(reversed(result))

        
    
    def update_cell(
        self,
        site_id: str,
        drive_name: str,
        file_path: str,
        sheet_name: str,
        range_address: str,
        value: Dict[str, Any],
        chunk_size: int = 0,
    ) -> Dict[str, Any]:
        # validate
        self._validate_non_empty(site_id=site_id, drive_name=drive_name, file_path=file_path,
                                 sheet_name=sheet_name, range_address=range_address)

        if not isinstance(value, dict) or not value:
            logging.error("Value must be a non-empty dict payload")
            raise ValueError("value must be a non-empty dict (e.g. {'values': [[...], [...]]})")

        file_name, folder_path = self._parse_file_path(file_path)

        try:
            _, item_id = self._get_drive_and_item(site_id, drive_name, folder_path, file_name)
            session_id = self._get_workbook_session_id(site_id, item_id)
            headers = self._build_headers(session_id)

            # No chunking requested or values not in expected 2D list shape
            values_matrix = value.get("values")
            if not chunk_size or not isinstance(values_matrix, list) or (values_matrix and not isinstance(values_matrix[0], list)):
                url = f"{BASE_URL}/sites/{site_id}/drive/items/{item_id}/workbook/worksheets/{sheet_name}/range(address='{range_address}')"
                logging.info("Updating range '%s' on sheet '%s' in file '%s'", range_address, sheet_name, file_name)
                return self._request("PATCH", url, headers=headers, json=value)

            # Parse start cell and end cell from range (e.g., A2:C100)
            def _parse_cell(cell: str) -> tuple[int, int]:
                import re
                m = re.match(r"^([A-Z]+)([0-9]+)$", cell)
                if not m:
                    raise ValueError(f"Invalid cell address: {cell}")
                col_letters, row_str = m.groups()
                # convert letters to index
                col_index = 0
                for ch in col_letters:
                    col_index = col_index * 26 + (ord(ch) - ord('A') + 1)
                return int(row_str), col_index

            def _cell_from(row: int, col_index: int) -> str:
                return f"{self.col_index_to_name(col_index)}{row}"

            if ":" not in range_address:
                raise ValueError("range_address must be an A1-style range like 'A2:C100' for chunking")
            start_addr, end_addr = range_address.split(":", 1)
            start_row, start_col_index = _parse_cell(start_addr)
            end_row, end_col_index = _parse_cell(end_addr)

            total_rows = len(values_matrix)
            failed_chunks: list[dict[str, Any]] = []
            responses: list[Dict[str, Any]] = []
            # Iterate in chunks of rows
            for chunk_start in range(0, total_rows, chunk_size):
                chunk_end = min(chunk_start + chunk_size, total_rows)
                # Compute Excel address rows for this chunk
                excel_start_row = start_row + chunk_start
                excel_end_row = start_row + chunk_end - 1
                start_cell = _cell_from(excel_start_row, start_col_index)
                end_cell = _cell_from(excel_end_row, end_col_index)
                chunk_range = f"{start_cell}:{end_cell}"
                chunk_values = values_matrix[chunk_start:chunk_end]
                url = f"{BASE_URL}/sites/{site_id}/drive/items/{item_id}/workbook/worksheets/{sheet_name}/range(address='{chunk_range}')"

                try:
                    logging.info("Updating chunk range '%s' on sheet '%s' in file '%s'", chunk_range, sheet_name, file_name)
                    resp = self._request("PATCH", url, headers=headers, json={"values": chunk_values})
                    responses.append({"range": chunk_range, "response": resp})
                except SharePointError as e:
                    logging.error("Chunk update failed for range '%s': %s", chunk_range, e)
                    failed_chunks.append({"range": chunk_range, "error": str(e)})

            result: Dict[str, Any] = {"ok": len(failed_chunks) == 0, "failed_chunks": failed_chunks, "chunks": responses}
            if failed_chunks:
                logging.warning("%d chunk(s) failed during update_cell", len(failed_chunks))
            return result
        except SharePointError:
            raise
        except Exception as e:
            logging.exception("Unexpected error updating cell")
            raise SharePointError(f"Unexpected error updating cell: {e}")


if __name__ == '__main__':
    import json
    try:
        sharepoint_manager = SharepointManager()
        excel_online_loader = ExcelOnlineLoader()
        
        site_id = sharepoint_manager.get_site_id('DE')
        # print(site_id)
        # drives = sharepoint_manager.get_all_drives_in_site(site_id)
        # drive_id = sharepoint_manager.get_drive_id(site_id, 'Documents')
        # range_address = None
        
        # response = excel_online_loader.get_used_range(site_id, 'Documents', 'Web Crawling/Walmart/Seller Center/buybox_report.xlsx', 'buybox_report', range_address=None)
        # column_to_update = excel_online_loader.col_index_to_name(response['column_count'] + 1)
        
        value = { "values": [ ["ID", "Name", "Age"], [1, "Alice", 25] ] }

        excel_online_loader.update_cell(site_id, 'Documents', 'Web Crawling/Walmart/Seller Center/buybox_report.xlsx', 'sheet2', 'A1:C2', value)
        pass
       


    except SharePointError as e:
        logging.error(f"SharePoint operation failed: {e}")
        print(f"Error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        print(f"Unexpected error: {e}")

 


                           