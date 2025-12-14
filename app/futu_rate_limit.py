"""Shared rate limiter for Futu API requests.

Tracks:
- Per 30-second window: max 60 requests (Futu API limit)
- Total quota: max 300 requests (configurable limit)

Note: This tracks our application's usage. Futu OpenD server also tracks quota
independently. If Futu OpenD shows quota reached, you may need to wait for
Futu's server-side quota to reset (typically resets daily or after a time period).
"""

from __future__ import annotations

import time
from collections import deque
from threading import Lock

# Rate limit configuration
MAX_REQUESTS_PER_30S = 60
MAX_TOTAL_QUOTA = 300
_MIN_REQUEST_INTERVAL = 0.6  # seconds between requests (conservative: ~50 req/30s)
_AUTO_RESET_INTERVAL = 24 * 3600  # Auto-reset after 24 hours (Futu typically resets daily)

# Global state (thread-safe)
_lock = Lock()
_request_times: deque[float] = deque()  # Timestamps of requests in last 30 seconds
_total_requests = 0
_quota_reset_time = time.time()  # Time when quota was last reset


def reset_quota(force: bool = False):
    """
    Reset the total request quota counter.
    
    Note: This only resets our application's tracking counter. If Futu OpenD
    server shows quota reached, you may need to wait for Futu's server-side
    quota to reset (typically resets daily).
    
    Args:
        force: If True, reset even if auto-reset hasn't occurred yet
    """
    global _total_requests, _quota_reset_time
    with _lock:
        _total_requests = 0
        _quota_reset_time = time.time()
        print(f"[futu_rate_limit] Quota reset. Note: This resets application tracking only. "
              "Futu OpenD server quota resets independently (typically daily).")


def get_quota_status() -> dict:
    """Get current quota status."""
    global _total_requests, _quota_reset_time, _request_times
    with _lock:
        current_time = time.time()
        # Clean old requests (older than 30 seconds)
        while _request_times and current_time - _request_times[0] > 30:
            _request_times.popleft()
        
        # Auto-reset if 24 hours have passed (matching Futu's typical reset cycle)
        time_since_reset = current_time - _quota_reset_time
        if time_since_reset >= _AUTO_RESET_INTERVAL:
            _total_requests = 0
            _quota_reset_time = current_time
            print(f"[futu_rate_limit] Auto-reset quota after 24 hours")
        
        requests_in_window = len(_request_times)
        remaining_30s = max(0, MAX_REQUESTS_PER_30S - requests_in_window)
        remaining_total = max(0, MAX_TOTAL_QUOTA - _total_requests)
        time_until_auto_reset = max(0, _AUTO_RESET_INTERVAL - time_since_reset)
        
        return {
            "requests_in_30s_window": requests_in_window,
            "remaining_30s_quota": remaining_30s,
            "total_requests": _total_requests,
            "remaining_total_quota": remaining_total,
            "quota_reset_time": _quota_reset_time,
            "time_until_auto_reset_seconds": time_until_auto_reset,
            "note": "This tracks application usage. Futu OpenD server has its own quota tracking that resets independently (typically daily).",
        }


def rate_limit():
    """
    Ensure we don't exceed Futu API rate limits:
    - Max 60 requests per 30 seconds
    - Max 300 total requests (until reset)
    
    Raises RuntimeError if quota exceeded.
    """
    global _total_requests, _request_times, _quota_reset_time
    
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
        
        # Auto-reset if 24 hours have passed
        time_since_reset = current_time - _quota_reset_time
        if time_since_reset >= _AUTO_RESET_INTERVAL:
            _total_requests = 0
            _quota_reset_time = current_time
            time_since_reset = 0  # Reset time_since_reset after auto-reset
        
        # Check total quota limit
        if _total_requests >= MAX_TOTAL_QUOTA:
            time_until_reset = _AUTO_RESET_INTERVAL - time_since_reset
            raise RuntimeError(
                f"Application quota tracking shows {MAX_TOTAL_QUOTA} requests reached.\n"
                f"Auto-reset in {time_until_reset/3600:.1f} hours, or call reset_quota() to reset now.\n"
                "Note: If Futu OpenD also shows quota reached, you may need to wait for Futu's server-side quota to reset (typically daily)."
            )
        
        # Enforce minimum interval between requests
        sleep_time = 0.0
        if _request_times:
            last_request = _request_times[-1]
            elapsed = current_time - last_request
            if elapsed < _MIN_REQUEST_INTERVAL:
                sleep_time = _MIN_REQUEST_INTERVAL - elapsed
        
        # Record this request (before releasing lock to ensure atomicity)
        _request_times.append(current_time)
        _total_requests += 1
    
    # Release lock before sleeping to avoid blocking other threads
    if sleep_time > 0:
        time.sleep(sleep_time)

