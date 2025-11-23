"""Shared rate limiter for Futu API requests.

Tracks:
- Per 30-second window: max 60 requests (Futu API limit)
- Total quota: max 300 requests (configurable limit)
"""

from __future__ import annotations

import time
from collections import deque
from threading import Lock

# Rate limit configuration
MAX_REQUESTS_PER_30S = 60
MAX_TOTAL_QUOTA = 300
_MIN_REQUEST_INTERVAL = 0.6  # seconds between requests (conservative: ~50 req/30s)

# Global state (thread-safe)
_lock = Lock()
_request_times: deque[float] = deque()  # Timestamps of requests in last 30 seconds
_total_requests = 0
_quota_reset_time = time.time()  # Time when quota was last reset


def reset_quota():
    """Reset the total request quota counter."""
    global _total_requests, _quota_reset_time
    with _lock:
        _total_requests = 0
        _quota_reset_time = time.time()


def get_quota_status() -> dict:
    """Get current quota status."""
    with _lock:
        current_time = time.time()
        # Clean old requests (older than 30 seconds)
        while _request_times and current_time - _request_times[0] > 30:
            _request_times.popleft()
        
        requests_in_window = len(_request_times)
        remaining_30s = max(0, MAX_REQUESTS_PER_30S - requests_in_window)
        remaining_total = max(0, MAX_TOTAL_QUOTA - _total_requests)
        
        return {
            "requests_in_30s_window": requests_in_window,
            "remaining_30s_quota": remaining_30s,
            "total_requests": _total_requests,
            "remaining_total_quota": remaining_total,
            "quota_reset_time": _quota_reset_time,
        }


def rate_limit():
    """
    Ensure we don't exceed Futu API rate limits:
    - Max 60 requests per 30 seconds
    - Max 300 total requests (until reset)
    
    Raises RuntimeError if quota exceeded.
    """
    global _total_requests, _request_times
    
    with _lock:
        current_time = time.time()
        
        # Clean old requests (older than 30 seconds)
        while _request_times and current_time - _request_times[0] > 30:
            _request_times.popleft()
        
        # Check 30-second window limit
        if len(_request_times) >= MAX_REQUESTS_PER_30S:
            oldest_request = _request_times[0]
            wait_time = 30 - (current_time - oldest_request) + 0.1  # Add small buffer
            if wait_time > 0:
                raise RuntimeError(
                    f"Futu API rate limit exceeded: {MAX_REQUESTS_PER_30S} requests per 30 seconds. "
                    f"Wait {wait_time:.1f} seconds before retrying."
                )
        
        # Check total quota limit
        if _total_requests >= MAX_TOTAL_QUOTA:
            raise RuntimeError(
                f"Futu API total quota exceeded: {MAX_TOTAL_QUOTA} requests. "
                "Call reset_quota() to reset the counter."
            )
        
        # Enforce minimum interval between requests
        if _request_times:
            last_request = _request_times[-1]
            elapsed = current_time - last_request
            if elapsed < _MIN_REQUEST_INTERVAL:
                sleep_time = _MIN_REQUEST_INTERVAL - elapsed
                time.sleep(sleep_time)
                current_time = time.time()
        
        # Record this request
        _request_times.append(current_time)
        _total_requests += 1

