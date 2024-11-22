# utils.py
import time
from collections import deque
from datetime import datetime, timedelta
import logging
import pandas as pd 
from . import config

class RateLimiter:
    def __init__(self, calls_per_minute=1200):
        self.calls_per_minute = calls_per_minute
        self.call_times = deque(maxlen=calls_per_minute)
    
    def __call__(self, func):
        def wrapper(*args, **kwargs):
            now = datetime.now()
            
            # Remove calls older than 1 minute
            while self.call_times and now - self.call_times[0] > timedelta(minutes=1):
                self.call_times.popleft()
            
            # If at limit, wait until oldest call is more than 1 minute ago
            if len(self.call_times) >= self.calls_per_minute:
                wait_time = (self.call_times[0] + timedelta(minutes=1) - now).total_seconds()
                if wait_time > 0:
                    time.sleep(wait_time)
            
            # Make the call
            result = func(*args, **kwargs)
            self.call_times.append(now)
            return result
            
        return wrapper

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def validate_data(df: pd.DataFrame) -> bool:
    return df is not None and len(df) >= config.DAYS_REQUIRED and not df.isnull().any().any()