"""Redis cache configuration and utilities."""

import redis
import json
import os
import time
import uuid
from typing import Optional, Any, Dict
from datetime import timedelta

# Redis configuration
REDIS_URL = os.getenv("REDIS_URL")

class CacheManager:
    """Redis cache manager."""
    
    def __init__(self, redis_url: str = REDIS_URL):
        self.redis_client = redis.from_url(redis_url, decode_responses=True)
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set a value in cache."""
        try:
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            return self.redis_client.set(key, value, ex=ttl)
        except Exception as e:
            print(f"Cache set error: {e}")
            return False
    
    def get(self, key: str) -> Optional[Any]:
        """Get a value from cache."""
        try:
            value = self.redis_client.get(key)
            if value:
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value
            return None
        except Exception as e:
            print(f"Cache get error: {e}")
            return None
    
    def delete(self, key: str) -> bool:
        """Delete a key from cache."""
        try:
            return bool(self.redis_client.delete(key))
        except Exception as e:
            print(f"Cache delete error: {e}")
            return False
    
    def exists(self, key: str) -> bool:
        """Check if key exists in cache."""
        try:
            return bool(self.redis_client.exists(key))
        except Exception as e:
            print(f"Cache exists error: {e}")
            return False
    

    
    def acquire_distributed_lock(self, lock_key: str, timeout: int = 30, blocking_timeout: int = 10) -> Optional[str]:
        """
        Acquire a distributed lock using Redis.
        
        Args:
            lock_key: The key for the lock
            timeout: Lock expiration time in seconds
            blocking_timeout: Maximum time to wait for lock acquisition
            
        Returns:
            Lock identifier if successful, None if failed
        """
        lock_identifier = str(uuid.uuid4())
        start_time = time.time()
        
        while time.time() - start_time < blocking_timeout:
            try:
                # Try to acquire lock with SET NX EX
                if self.redis_client.set(lock_key, lock_identifier, nx=True, ex=timeout):
                    return lock_identifier
                
                # Wait a bit before retrying
                time.sleep(0.1)
            except Exception as e:
                print(f"Distributed lock acquisition error: {e}")
                return None
        
        return None
    
    def release_distributed_lock(self, lock_key: str, lock_identifier: str) -> bool:
        """
        Release a distributed lock using Lua script for atomicity.
        
        Args:
            lock_key: The key for the lock
            lock_identifier: The lock identifier returned by acquire_distributed_lock
            
        Returns:
            True if lock was released, False otherwise
        """
        # Lua script to atomically check and delete the lock
        lua_script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        else
            return 0
        end
        """
        
        try:
            result = self.redis_client.eval(lua_script, 1, lock_key, lock_identifier)
            return bool(result)
        except Exception as e:
            print(f"Distributed lock release error: {e}")
            return False
    
    def is_distributed_lock_held(self, lock_key: str) -> bool:
        """
        Check if a distributed lock is currently held.
        
        Args:
            lock_key: The key for the lock
            
        Returns:
            True if lock is held, False otherwise
        """
        try:
            return bool(self.redis_client.exists(lock_key))
        except Exception as e:
            print(f"Distributed lock check error: {e}")
            return False
    

    


# Global cache instance
cache = CacheManager()

