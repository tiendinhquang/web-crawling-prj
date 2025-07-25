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


class ExcelOnlineLoader(BaseExcelOnlineLoader):
    """
    Concrete implementation of Excel Online loader.
    
    Provides methods to interact with Excel files stored in SharePoint
    through Microsoft Graph API.
    """
    
    def __init__(self):
        """
        Initialize the Excel Online loader.
        
        Raises:
            SharePointError: If authentication fails
        """
        super().__init__()
        logging.info("Excel Online loader initialized successfully")
    
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
            Dict[str, Any]: Excel range data containing values and properties
            
        Raises:
            SharePointError: If any operation fails
            ValueError: If any parameter is empty or invalid
        """
        if not site_id or not site_id.strip():
            logging.error("Site ID cannot be empty")
            raise ValueError("Site ID cannot be empty")
        
        if not drive_name or not drive_name.strip():
            logging.error("Drive name cannot be empty")
            raise ValueError("Drive name cannot be empty")
        
        if not file_path or not file_path.strip():
            logging.error("File path cannot be empty")
            raise ValueError("File path cannot be empty")
        
        if not sheet_name or not sheet_name.strip():
            logging.error("Sheet name cannot be empty")
            raise ValueError("Sheet name cannot be empty")
        
        if not range_address or not range_address.strip():
            logging.error("Range address cannot be empty")
            raise ValueError("Range address cannot be empty")
        
        try:
            # Extract file name and folder path
            file_name = file_path.split('/')[-1]
            folder_path = file_path.rstrip(file_name)
            
            # Get drive and item IDs
            sharepoint_manager = SharepointManager()
            drive_id = sharepoint_manager.get_drive_id(site_id, drive_name)
            
            if not drive_id:
                logging.error(f"Drive '{drive_name}' not found in site ID: {site_id}")
                raise SharePointError(f"Drive '{drive_name}' not found in site ID: {site_id}")
            
            item_id = sharepoint_manager.get_item_id(site_id, drive_id, folder_path, file_name)
            
            if not item_id:
                logging.error(f"File '{file_name}' not found in folder: {folder_path}")
                raise SharePointError(f"File '{file_name}' not found in folder: {folder_path}")
            
            # Get workbook session
            session_id = self._get_workbook_session_id(site_id, item_id)
            
            # Get used range
            url = f"{BASE_URL}/sites/{site_id}/drive/items/{item_id}/workbook/worksheets/{sheet_name}/range(address='{range_address}')/usedRange"
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Workbook-Session-Id": session_id
            }
            
            logging.info(f"Fetching used range '{range_address}' from sheet '{sheet_name}' in file '{file_name}'")
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            range_data = response.json()
            logging.info(f"Successfully retrieved used range data from sheet '{sheet_name}'")
            return range_data
            
        except SharePointError:
            # Re-raise SharePointError from other methods
            raise
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to get used range from Excel file: {e}")
            raise SharePointError(f"Failed to get used range from Excel file: {e}")
        except Exception as e:
            logging.error(f"Unexpected error getting used range: {e}")
            raise SharePointError(f"Unexpected error getting used range: {e}")


if __name__ == '__main__':
    try:
        sharepoint_manager = SharepointManager()
        excel_online_loader = ExcelOnlineLoader()
        
        site_id = sharepoint_manager.get_site_id('DE')
        drives = sharepoint_manager.get_all_drives_in_site(site_id)
        drive_id = sharepoint_manager.get_drive_id(site_id, 'Data Backup')
        
        if drive_id:
            print(f"Drive ID: {drive_id}")
            items = sharepoint_manager.get_items_in_folder(site_id, drive_id, 'Web Crawling/Wayfair')
            print(f"Items in folder: {items}")
        else:
            print("Drive 'Documents' not found")
            
    except SharePointError as e:
        logging.error(f"SharePoint operation failed: {e}")
        print(f"Error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        print(f"Unexpected error: {e}")

 


                           