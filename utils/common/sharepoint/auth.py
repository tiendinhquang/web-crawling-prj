import yaml
from msal import ConfidentialClientApplication
from abc import ABC, abstractmethod
import json
import time
import os

# Load cấu hình từ YAML
with open('config/azure.yaml', 'r') as f:
    data = yaml.safe_load(f)

CLIENT_ID = data['APP_ID']
CLIENT_SECRET = data['CLIENT_SECRET']
TENANT_ID = data['DIRECTORY_ID']
SHAREPOINT_HOST = 'atlasintl.sharepoint.com'

AUTH_CACHE_PATH = 'config/.azure_token_cache.json'

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

    def _is_token_expired(self):
        if not os.path.exists(AUTH_CACHE_PATH):
            return True
        try:
            with open(AUTH_CACHE_PATH, 'r') as f:
                data = json.load(f)
            return time.time() >= data.get('expires_on', 0)
        except Exception as e:
            print(f"[Token Cache Error] {e}")
            return True

    def get_access_token(self):
        if self._is_token_expired():
            result = self.app.acquire_token_for_client(scopes=self.scopes)
            if "access_token" not in result:
                raise Exception(f"Failed to acquire token: {result}")
            result['expires_on'] = time.time() + result['expires_in']
            with open(AUTH_CACHE_PATH, 'w') as f:
                json.dump(result, f, indent=4)
        else:
            with open(AUTH_CACHE_PATH, 'r') as f:
                result = json.load(f)
        return result

# Gọi thử để in access token
# if __name__ == "__main__":        
#     auth = AzureAuth()
#     print(auth.get_access_token()['access_token'])
