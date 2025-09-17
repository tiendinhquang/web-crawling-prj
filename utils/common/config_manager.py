import os
import json
import logging
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from typing import Dict, Any, Optional, List, Union
import yaml
from pathlib import Path
import sys
from filelock import FileLock

logging.basicConfig(level=logging.INFO)
logging = logging.getLogger(__name__)


load_dotenv()


BASE_DIR = Path(__file__).resolve().parents[2]   


sys.path.append(str(BASE_DIR))


class ConfigError(Exception):
    """Custom exception for configuration operations."""
    pass


# ==== ConfigManager Abstract ====
class ConfigLoader(ABC):
    """
    Abstract base class for configuration loaders.
    
    Provides a common interface for loading configuration from different file formats.
    Subclasses must implement the load_config method.
    """
    
    def __init__(self, config_path: str):
        """
        Initialize the configuration loader.
        
        Args:
            config_path: Path to the configuration file
            
        Raises:
            ValueError: If config_path is empty or invalid
        """
        if not config_path or not config_path.strip():
            logging.error("Configuration path cannot be empty")
            raise ValueError("Configuration path cannot be empty")
        
        self.config_path = config_path
        self.config = self.load_config()
        # logging.info(f"ConfigLoader initialized with path: {config_path}")
        
    @abstractmethod
    def load_config(self) -> Optional[Dict[str, Any]]:
        """
        Load configuration from the specified file.
        
        Returns:
            Optional[Dict[str, Any]]: Configuration data or None if loading fails
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        pass


# ==== ConfigLoader Implementations ====
class JsonConfigLoader(ConfigLoader):
    """
    Concrete implementation of ConfigLoader for JSON files.
    
    Handles loading and parsing of JSON configuration files with proper error handling.
    """
    
    def load_config(self) -> Optional[Dict[str, Any]]:
        """
        Load configuration from a JSON file.
        
        Returns:
            Optional[Dict[str, Any]]: Parsed JSON configuration or None if loading fails
            
        Raises:
            ConfigError: If file cannot be read or parsed
        """
        try:
            if not os.path.exists(self.config_path):
                logging.error(f"Configuration file not found: {self.config_path}")
                raise ConfigError(f"Configuration file not found: {self.config_path}")
            
            with open(self.config_path, "r", encoding="utf-8") as f:
                config_data = json.load(f)
            
            if not isinstance(config_data, dict):
                logging.error(f"Invalid JSON structure in {self.config_path}: expected dict, got {type(config_data)}")
                raise ConfigError(f"Invalid JSON structure in {self.config_path}: expected dict")
            
            # logging.info(f"Successfully loaded JSON configuration from {self.config_path}")
            return config_data
            
        except FileNotFoundError:
            logging.error(f"File not found: {self.config_path}")
            raise ConfigError(f"File not found: {self.config_path}")
        except PermissionError as e:
            logging.error(f"Permission denied reading file {self.config_path}: {e}")
            raise ConfigError(f"Permission denied reading file {self.config_path}: {e}")
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON file {self.config_path}: {e}")
            raise ConfigError(f"Error decoding JSON file {self.config_path}: {e}")
        except UnicodeDecodeError as e:
            logging.error(f"Unicode decode error reading file {self.config_path}: {e}")
            raise ConfigError(f"Unicode decode error reading file {self.config_path}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error loading JSON config from {self.config_path}: {e}")
            raise ConfigError(f"Unexpected error loading JSON config from {self.config_path}: {e}")


class YamlConfigLoader(ConfigLoader):
    """
    Concrete implementation of ConfigLoader for YAML files.
    
    Handles loading and parsing of YAML configuration files with proper error handling.
    """
    
    def load_config(self) -> Optional[Dict[str, Any]]:
        """
        Load configuration from a YAML file.
        
        Returns:
            Optional[Dict[str, Any]]: Parsed YAML configuration or None if loading fails
            
        Raises:
            ConfigError: If file cannot be read or parsed
        """
        try:
            if not os.path.exists(self.config_path):
                logging.error(f"Configuration file not found: {self.config_path}")
                raise ConfigError(f"Configuration file not found: {self.config_path}")
            
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
            
            if not isinstance(config_data, dict):
                logging.error(f"Invalid YAML structure in {self.config_path}: expected dict, got {type(config_data)}")
                raise ConfigError(f"Invalid YAML structure in {self.config_path}: expected dict")
            
            logging.info(f"Successfully loaded YAML configuration from {self.config_path}")
            return config_data
            
        except FileNotFoundError:
            logging.error(f"File not found: {self.config_path}")
            raise ConfigError(f"File not found: {self.config_path}")
        except PermissionError as e:
            logging.error(f"Permission denied reading file {self.config_path}: {e}")
            raise ConfigError(f"Permission denied reading file {self.config_path}: {e}")
        except yaml.YAMLError as e:
            logging.error(f"Error decoding YAML file {self.config_path}: {e}")
            raise ConfigError(f"Error decoding YAML file {self.config_path}: {e}")
        except UnicodeDecodeError as e:
            logging.error(f"Unicode decode error reading file {self.config_path}: {e}")
            raise ConfigError(f"Unicode decode error reading file {self.config_path}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error loading YAML config from {self.config_path}: {e}")
            raise ConfigError(f"Unexpected error loading YAML config from {self.config_path}: {e}")


# ==== Specialized Config Loaders ====
class HeaderConfigLoader(JsonConfigLoader):
    """
    Specialized loader for HTTP header configurations.
    
    Extends JsonConfigLoader to provide header-specific functionality.
    """
    
    def load(self, header_name: str) -> Dict[str, Any]:
        """
        Load header configuration for a specific header name.
        
        Args:
            header_name: Name of the header configuration to load
            
        Returns:
            Dict[str, Any]: Header configuration dictionary
            
        Raises:
            ValueError: If header_name is empty or invalid
        """
        if not header_name or not header_name.strip():
            logging.error("Header name cannot be empty")
            raise ValueError("Header name cannot be empty")
        
        try:
            if not self.config:
                logging.warning(f"No configuration loaded, returning empty dict for header: {header_name}")
                return {}
            
            header_config = self.config.get(header_name, None)
            if not header_config:
                logging.warning(f"Header configuration not found for: {header_name}")
            
            logging.debug(f"Loaded header configuration for '{header_name}': {len(header_config)} items")
            return header_config
            
        except Exception as e:
            logging.error(f"Error loading header configuration for '{header_name}': {e}")
            return {}

    def update_header(self, header_name: str, new_headers: Dict[str, Any]) -> bool:
        """
        Update header configuration for a specific header name with file locking.
        
        Args:
            header_name: Name of the header configuration to update
            new_headers: New header configuration dictionary
            
        Returns:
            bool: True if update was successful, False otherwise
            
        Raises:
            ValueError: If header_name is empty or invalid
            ConfigError: If update operation fails
        """
        if not header_name or not header_name.strip():
            logging.error("Header name cannot be empty")
            raise ValueError("Header name cannot be empty")
        
        if not isinstance(new_headers, dict):
            logging.error("New headers must be a dictionary")
            raise ValueError("New headers must be a dictionary")
        
        # Create lock file path
        lock_file = f"{self.config_path}.lock"
        
        try:
            with FileLock(lock_file, timeout=10):
                # Reload config to get latest version
                self.config = self.load_config()
                
                if not self.config:
                    logging.error("No configuration loaded, cannot update headers")
                    raise ConfigError("No configuration loaded, cannot update headers")
                
                # Update the header configuration
                self.config[header_name] = new_headers
                
                # Save the updated configuration back to file
                with open(self.config_path, 'w', encoding='utf-8') as f:
                    json.dump(self.config, f, indent=4, ensure_ascii=False)
                
                logging.info(f"Successfully updated header configuration for '{header_name}' with {len(new_headers)} items")
                return True
                
        except Exception as e:
            logging.error(f"Error updating header configuration for '{header_name}': {e}")
            raise ConfigError(f"Error updating header configuration for '{header_name}': {e}")



class CookieConfigLoader(JsonConfigLoader):
    """
    Specialized loader for cookie configurations.
    
    Extends JsonConfigLoader to provide cookie-specific functionality.
    """
    
    def load(self, cookie_name: str) -> Dict[str, Any]:
        """
        Load cookie configuration for a specific cookie name.
        
        Args:
            cookie_name: Name of the cookie configuration to load
            
        Returns:
            Dict[str, Any]: Cookie configuration dictionary
            
        Raises:
            ValueError: If cookie_name is empty or invalid
        """        
        try:
            if not self.config:
                logging.warning(f"No configuration loaded, returning empty dict for cookie: {cookie_name}")
                return {}
            
            cookie_config = self.config.get(cookie_name, None)
            if not cookie_config:
                logging.warning(f"Cookie configuration not found for: {cookie_name}")
            
            return cookie_config
            
        except Exception as e:
            logging.error(f"Error loading cookie configuration for '{cookie_name}': {e}")
            return {}

    def update_cookie(self, cookie_name: str, new_cookies: Dict[str, Any]) -> bool:
        """
        Update cookie configuration for a specific cookie name with file locking.
        
        Args:
            cookie_name: Name of the cookie configuration to update
            new_cookies: New cookie configuration dictionary
            
        Returns:
            bool: True if update was successful, False otherwise
            
        Raises:
            ValueError: If cookie_name is empty or invalid
            ConfigError: If update operation fails
        """
        if not cookie_name or not cookie_name.strip():
            logging.error("Cookie name cannot be empty")
            raise ValueError("Cookie name cannot be empty")
        
        if not isinstance(new_cookies, dict):
            logging.error("New cookies must be a dictionary")
            raise ValueError("New cookies must be a dictionary")
        
        # Create lock file path
        lock_file = f"{self.config_path}.lock"
        
        try:
            with FileLock(lock_file, timeout=10):
                # Reload config to get latest version
                self.config = self.load_config()
                
                if not self.config:
                    logging.error("No configuration loaded, cannot update cookies")
                    raise ConfigError("No configuration loaded, cannot update cookies")
                
                # Update the cookie configuration
                self.config[cookie_name] = new_cookies
                
                # Save the updated configuration back to file
                with open(self.config_path, 'w', encoding='utf-8') as f:
                    json.dump(self.config, f, indent=4, ensure_ascii=False)
                
                logging.info(f"Successfully updated cookie configuration for '{cookie_name}' with {len(new_cookies)} items")
                return True
                
        except Exception as e:
            logging.error(f"Error updating cookie configuration for '{cookie_name}': {e}")
            raise ConfigError(f"Error updating cookie configuration for '{cookie_name}': {e}")




class MappingConfigLoader(YamlConfigLoader):
    """
    Specialized loader for database mapping configurations.
    
    Extends YamlConfigLoader to provide mapping-specific functionality.
    """
    
    def load(self, table_code: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Load mapping configuration for a specific table.
        
        Args:
            table_code: Code/name of the table configuration to load
            
        Returns:
            Optional[Dict[str, Any]]: Table mapping configuration or None if not found
            
        Raises:
            ValueError: If table_code is invalid when provided
        """
        try:
            if table_code is not None and (not table_code or not table_code.strip()):
                logging.error("Table code cannot be empty when provided")
                raise ValueError("Table code cannot be empty when provided")
            
            if not self.config:
                logging.warning("No configuration loaded, returning None for mapping")
                return None
            
            warehouse_config = self.config.get("warehouse", {})
            if not warehouse_config:
                logging.warning("No warehouse configuration found in mapping file")
                return None
            
            if table_code:
                mapping_config = warehouse_config.get(table_code, {})
                if not mapping_config:
                    logging.warning(f"Table mapping configuration not found for: {table_code}")
                else:
                    logging.debug(f"Loaded mapping configuration for table '{table_code}': {len(mapping_config)} items")
                return mapping_config
            else:
                logging.debug(f"Returning full warehouse configuration: {len(warehouse_config)} tables")
                return warehouse_config
            
        except Exception as e:
            logging.error(f"Error loading mapping configuration for table '{table_code}': {e}")
            return None


# ==== Wrapper Functions ====
def get_header_config(header_name: str, path= None) -> Dict[str, Any]:
    """
    Convenience function to get header configuration.
    
    Args:
        header_name: Name of the header configuration to load
        
    Returns:
        Dict[str, Any]: Header configuration dictionary
        
    Raises:
        ConfigError: If configuration loading fails
        ValueError: If header_name is invalid
    """
    try:
        if not BASE_DIR:
            logging.error("BASE_DIR environment variable not set")
            raise ConfigError("BASE_DIR environment variable not set")
        
        config_path = os.path.join(BASE_DIR, "config", "headers_config.json")
        config_loader = HeaderConfigLoader(config_path)
        return config_loader.load(header_name)
        
    except ConfigError:
        # Re-raise ConfigError from HeaderConfigLoader
        raise
    except Exception as e:
        logging.error(f"Unexpected error getting header config for '{header_name}': {e}")
        raise ConfigError(f"Unexpected error getting header config for '{header_name}': {e}")


def update_header_config(header_name: str, new_headers: Dict[str, Any], path= None) -> bool:
    """
    Convenience function to update header configuration.
    
    Args:
        header_name: Name of the header configuration to update
        new_headers: New header configuration dictionary
        
    Returns:
        bool: True if update was successful, False otherwise
        
    Raises:
        ConfigError: If configuration update fails
        ValueError: If header_name or new_headers are invalid
    """

    try:
        if not BASE_DIR:
            logging.error("BASE_DIR environment variable not set")
            raise ConfigError("BASE_DIR environment variable not set")
        
        if not path:
            config_path = os.path.join(BASE_DIR, "config", "headers_config.json")
        else:
            config_path = path
        
        config_loader = HeaderConfigLoader(config_path)
        return config_loader.update_header(header_name, new_headers)

        
    except ConfigError:
        # Re-raise ConfigError from HeaderConfigLoader
        raise
    except Exception as e:
        logging.error(f"Unexpected error updating header config for '{header_name}': {e}")
        raise ConfigError(f"Unexpected error updating header config for '{header_name}': {e}")

def get_cookie_config(cookie_name: str, path= None) -> Dict[str, Any]:
    """
    Convenience function to get cookie configuration.
    
    Args:
        cookie_name: Name of the cookie configuration to load
        
    Returns:
        Dict[str, Any]: Cookie configuration dictionary
        
    Raises:
        ConfigError: If configuration loading fails
        ValueError: If cookie_name is invalid
    """
    try:
        if not BASE_DIR:
            logging.error("BASE_DIR environment variable not set")
            raise ConfigError("BASE_DIR environment variable not set")
        
        if not path:
            config_path = os.path.join(BASE_DIR, "config", "cookies_config.json")
        else:
            config_path = path
            
        config_loader = CookieConfigLoader(config_path)
        return config_loader.load(cookie_name)
        
    except ConfigError:
        # Re-raise ConfigError from CookieConfigLoader
        raise
    except Exception as e:
        logging.error(f"Unexpected error getting cookie config for '{cookie_name}': {e}")
        raise ConfigError(f"Unexpected error getting cookie config for '{cookie_name}': {e}")


def update_cookie_config(cookie_name: str, new_cookies: Dict[str, Any], path= None) -> bool:
    """
    Convenience function to update cookie configuration.
    
    Args:
        cookie_name: Name of the cookie configuration to update
        new_cookies: New cookie configuration dictionary
        
    Returns:
        bool: True if update was successful, False otherwise
        
    Raises:
        ConfigError: If configuration update fails
        ValueError: If cookie_name or new_cookies are invalid
    """
    try:
        if not BASE_DIR:
            logging.error("BASE_DIR environment variable not set")
            raise ConfigError("BASE_DIR environment variable not set")
        
        if not path:
            config_path = os.path.join(BASE_DIR, "config", "cookies_config.json")
        else:
            config_path = path
            
        config_loader = CookieConfigLoader(config_path)
        return config_loader.update_cookie(cookie_name, new_cookies)
        
    except ConfigError:
        # Re-raise ConfigError from CookieConfigLoader
        raise
    except Exception as e:
        logging.error(f"Unexpected error updating cookie config for '{cookie_name}': {e}")
        raise ConfigError(f"Unexpected error updating cookie config for '{cookie_name}': {e}")


def get_mapping_config(table_code: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Convenience function to get mapping configuration.
    
    Args:
        table_code: Optional code/name of the table configuration to load
        
    Returns:
        Optional[Dict[str, Any]]: Table mapping configuration or None if not found
        
    Raises:
        ConfigError: If configuration loading fails
        ValueError: If table_code is invalid when provided
    """
    try:
        if not BASE_DIR:
            logging.error("BASE_DIR environment variable not set")
            raise ConfigError("BASE_DIR environment variable not set")
        
        config_path = os.path.join(BASE_DIR, "config", "mapping.yaml")
        config_loader = MappingConfigLoader(config_path)
        return config_loader.load(table_code)
        
    except ConfigError:
        # Re-raise ConfigError from MappingConfigLoader
        raise
    except Exception as e:
        logging.error(f"Unexpected error getting mapping config for table '{table_code}': {e}")
        raise ConfigError(f"Unexpected error getting mapping config for table '{table_code}': {e}")


# Example usage and testing
from concurrent.futures import ThreadPoolExecutor, as_completed

def run_get_header(name: str):
    config = get_cookie_config(name)
    return name, config

if __name__ == '__main__':
    header_names = [
        'wayfair_product_info','wayfair_product_info','wayfair_product_info','wayfair_product_info'
    ]

    results = {}
    with ThreadPoolExecutor(max_workers=4) as executor:  # max_workers = số thread
        futures = {executor.submit(run_get_header, name): name for name in header_names}
        for future in as_completed(futures):
            name, config = future.result()
            results[name] = config
            print(f"{name}: {config}")
