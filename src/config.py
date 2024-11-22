# config.py
from pathlib import Path
import pandas as pd

# API Configuration
BINANCE_API_KEY = 'dyBeWtoHCZ58m4UqjhVs5sAKgNcInVlNqD5HfKqk51YcIwNtSwCM38pGmmt8LRBQ'
BINANCE_SECRET_KEY = 'V0BhFXHVoknTcZ6SuY4qFIZkKAfS6fsTbydrvqO0kYcr5digu1MBscKYCzX8jVJE'

# Data Parameters
CACHE_DIR = Path('data/cache')
BACKUP_DIR = CACHE_DIR / 'backups'
CACHE_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR.mkdir(parents=True, exist_ok=True)

# Rate Limiting
MAX_CALLS_PER_MINUTE = 1200
RETRY_ATTEMPTS = 3

# Market Parameters
QUOTE_CURRENCY = 'USDT'
MIN_VOLUME = 1_000_000  # Minimum daily volume in USDT
MIN_TRADES = 1000      # Minimum daily trades
MAX_SYMBOLS = 300      # Maximum number of symbols to track
DATA_VERSION = '1.0'   # For versioning cached data

# Validation Parameters
DAYS_REQUIRED = 360    # Minimum days of data required
MAX_ALLOWED_GAPS = 5   # Maximum number of allowed missing days

