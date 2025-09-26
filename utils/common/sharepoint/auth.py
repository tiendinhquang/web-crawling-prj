import yaml
from msal import ConfidentialClientApplication
from abc import ABC, abstractmethod
import json
import time
import os
from utils.cache import CacheManager

# Load cấu hình từ YAML
with open('config/azure.yaml', 'r') as f:
    data = yaml.safe_load(f)

CLIENT_ID = data['APP_ID']
CLIENT_SECRET = data['CLIENT_SECRET']
TENANT_ID = data['DIRECTORY_ID']
SHAREPOINT_HOST = 'atlasintl.sharepoint.com'

# Redis cache key for Azure token
AUTH_CACHE_KEY = 'azure_token_cache'

# Base class sử dụng abstract method
class BaseAzureAuth(ABC):
    def __init__(self):
        self.authority = f"https://login.microsoftonline.com/{TENANT_ID}"
        self.scopes = ["https://graph.microsoft.com/.default"]
        self.app = ConfidentialClientApplication(
            client_id=CLIENT_ID,
            client_credential=CLIENT_SECRET,
            authority=self.authority
        )

    @abstractmethod
    def _is_token_expired(self):
        pass

    @abstractmethod
    def get_access_token(self):
        pass

# Class kế thừa, cài đặt kiểm tra và lấy token
class AzureAuth(BaseAzureAuth):
    def __init__(self):
        super().__init__()
        self.cache = CacheManager()

    def _is_token_expired(self):
        try:
            cached_data = self.cache.get(AUTH_CACHE_KEY)
            if not cached_data:
                return True
            return time.time() >= cached_data.get('expires_on', 0)
        except Exception as e:
            print(f"[Token Cache Error] {e}")
            return True

    def get_access_token(self):
        if self._is_token_expired():
            result = self.app.acquire_token_for_client(scopes=self.scopes)
            if "access_token" not in result:
                raise Exception(f"Failed to acquire token: {result}")
            result['expires_on'] = time.time() + result['expires_in']
            # Store in Redis with TTL slightly less than actual expiration
            ttl = int(result['expires_in'] - 60)  # 60 seconds buffer
            self.cache.set(AUTH_CACHE_KEY, result, ttl=ttl)
        else:
            result = self.cache.get(AUTH_CACHE_KEY)
        return result

