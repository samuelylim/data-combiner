"""
Rate limiter for managing API request timing.

This module provides a RateLimiter class that can enforce rate limits
for API requests, supporting both per-endpoint and shared rate limits.
"""

import asyncio
import time
from typing import Dict, Optional
from collections import deque


class RateLimiter:
    """
    Rate limiter that tracks request timing and enforces rate limits.
    
    Supports:
    - Per-limiter rate limits (requests per minute)
    - Shared rate limits via shared_limit_key
    - Async-safe with proper locking
    """
    
    # Class-level storage for shared rate limiters
    _shared_limiters: Dict[str, 'RateLimiter'] = {}
    
    def __init__(self, requests_per_minute: int):
        """
        Initialize a rate limiter.
        
        Args:
            requests_per_minute: Maximum number of requests allowed per minute
        """
        self.requests_per_minute = requests_per_minute
        self.min_interval = 60.0 / requests_per_minute  # Seconds between requests
        self.request_times: deque = deque()  # Track recent request timestamps
        self.lock = asyncio.Lock()  # Ensure thread-safety
    
    @classmethod
    def get_limiter(cls, requests_per_minute: int, shared_limit_key: Optional[str] = None) -> 'RateLimiter':
        """
        Get or create a rate limiter, optionally sharing across multiple sources.
        
        Args:
            requests_per_minute: Maximum requests per minute
            shared_limit_key: Optional key for shared rate limit (e.g., 'example.com')
            
        Returns:
            A RateLimiter instance (shared or new)
        """
        if shared_limit_key:
            # Use or create shared limiter
            if shared_limit_key not in cls._shared_limiters:
                cls._shared_limiters[shared_limit_key] = cls(requests_per_minute)
            return cls._shared_limiters[shared_limit_key]
        else:
            # Create new limiter for this source only
            return cls(requests_per_minute)
    
    async def acquire(self) -> None:
        """
        Wait until a request can be made within the rate limit.
        
        This method will block (sleep) if necessary to enforce the rate limit.
        """
        async with self.lock:
            current_time = time.time()
            
            # Remove timestamps older than 1 minute
            cutoff_time = current_time - 60.0
            while self.request_times and self.request_times[0] < cutoff_time:
                self.request_times.popleft()
            
            # If we're at the limit, wait until the oldest request expires
            if len(self.request_times) >= self.requests_per_minute:
                oldest_time = self.request_times[0]
                wait_time = (oldest_time + 60.0) - current_time
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    # Remove the expired timestamp
                    self.request_times.popleft()
            
            # Also enforce minimum interval between requests
            if self.request_times:
                last_request_time = self.request_times[-1]
                time_since_last = current_time - last_request_time
                if time_since_last < self.min_interval:
                    wait_time = self.min_interval - time_since_last
                    await asyncio.sleep(wait_time)
            
            # Record this request
            self.request_times.append(time.time())
