import logging
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv
from utils.cache import cache  # Redis-based cache
from typing import Optional, Dict, Any
import yaml
# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
import os
load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[2]


# ==== Custom Exceptions ====
class ConfigError(Exception):
    """Custom exception for configuration errors."""
    pass


# ==== Config Manager ====
class ConfigManager:
    HEADERS = {

        'headers:gg_ads_auction_insights',
        'headers:gg_merchants_create_report',
        'headers:gg_merchants_get_report_status',
        'headers:gg_merchants_get_report',
        'headers:walmart_ad',
        'headers:walmart_seller',
        'headers:lowes_vendor',
        'headers:wayfair_product_info',
        'headers:wayfair_product_detail',
        'headers:wayfair_product_dimension',
        'headers:wayfair_product_reviews',
        'headers:wayfair_product_specification',
        'headers:wayfair_product_list',
        'headers:walmart_product_list',
        'headers:walmart_product_src_page',
        'headers:walmart_product_detail_page',
        'headers:criteo',
    }

    COOKIES = {
        "cookies:gg_ads",
        "cookies:gg_merchants",
        "cookies:wayfair_product_detail",
        "cookies:lowes_vendor",
        "cookies:walmart_seller",
        "cookies:walmart_ad",
        "cookies:gg_merchants",
        "cookies:gg_ads",
        "cookies:wayfair_pdp",
    }

    def __init__(self):
        self.cache = cache

    def get_cfg_from_cache(self, name: str, category: str) -> Dict[str, Any]:
        """Generic getter for config (headers/cookies)."""
        self._validate_name(name, category)
        try:
            return self.cache.get(name)
        except Exception as e:
            logger.error(f"Error getting {category} config '{name}': {e}")
            raise ConfigError(str(e))

    def update(self, name: str, new_data: Dict[str, Any], category: str) -> bool:
        """Generic updater for config (headers/cookies)."""
        self._validate_name(name, category)
        if not isinstance(new_data, dict):
            raise ConfigError("Config data must be a dictionary")
        try:
            self.cache.set(name, new_data)
            logging.info(f"Successfully updated {category} configuration for '{name}' with {len(new_data)} items")
            return True
        except Exception as e:
            logging.error(f"Error loading header configuration for '{name}': {e}")
            return {}
    def _validate_name(self, name: str, category: str) -> None:
        if name not in self.HEADERS and name not in self.COOKIES:
            raise ConfigError(f"{category} configuration for '{name}' not found")
    def get_config(self, path: str) -> Dict[str, Any]:
        with open(path, "r") as f:
            return yaml.safe_load(f)




# ==== Wrapper Functions ====
config_manager = ConfigManager()

def get_header_config(name: str) -> Dict[str, Any]:
    return config_manager.get_cfg_from_cache(name, "header")

def update_header_config(name: str, data: Dict[str, Any]) -> bool:
    return config_manager.update(name, data, "header")

def get_cookie_config(name: str) -> Dict[str, Any]:
    return config_manager.get_cfg_from_cache(name, "cookie")

def update_cookie_config(name: str, data: Dict[str, Any]) -> bool:
    return config_manager.update(name, data, "cookie")


def get_mapping_config(table_code: Optional[str] = None) -> Optional[Dict[str, Any]]:
    try:
        if not BASE_DIR:
            logging.error("BASE_DIR environment variable not set")
            raise ConfigError("BASE_DIR environment variable not set")
        
        config_path = os.path.join(BASE_DIR, "config", "mapping.yaml")
        mapping = config_manager.get_config(config_path)
        mapping['warehouse'][table_code]
        return mapping['warehouse'][table_code]
    except KeyError:
        raise ConfigError(f"Mapping configuration for table '{table_code}' not found")
    except Exception as e:
        logging.error(f"Unexpected error getting mapping config for table '{table_code}': {e}")
        raise ConfigError(f"Unexpected error getting mapping config for table '{table_code}': {e}")


