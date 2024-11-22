# data.py
from binance.client import Client
import pandas as pd
from datetime import datetime, timedelta
import logging
from pathlib import Path
import time
from typing import Dict, List


from . import config
from . import utils
from .storage import DataStorage


logger = logging.getLogger(__name__)

rate_limit = utils.RateLimiter(calls_per_minute=config.MAX_CALLS_PER_MINUTE)


class DataManager:
    def __init__(self):
        self.client = Client(config.BINANCE_API_KEY, config.BINANCE_SECRET_KEY)
        self.storage = DataStorage()
        self.cache = {}
    
    @rate_limit
    def _api_call(self, func, *args, **kwargs):
        """Wrapper for rate-limited API calls with retries"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
        
    def get_tradeable_symbols(self) -> list:
        """Get list of valid USDT pairs meeting volume requirements"""
        try:
            info = self._api_call(self.client.get_exchange_info)
            tickers = self._api_call(self.client.get_ticker)
            
            # Create ticker lookup
            ticker_data = {t['symbol']: t for t in tickers}
            
            valid_symbols = []
            for symbol in info['symbols']:
                if (symbol['status'] == 'TRADING' and
                    symbol['quoteAsset'] == 'USDT' and
                    symbol['symbol'] in ticker_data):
                    
                    volume = float(ticker_data[symbol['symbol']]['quoteVolume'])
                    if volume >= config.MIN_VOLUME:
                        valid_symbols.append(symbol['symbol'])
                        
            return sorted(valid_symbols)[:config.MAX_SYMBOLS]
            
        except Exception as e:
            logger.error(f"Error fetching symbols: {e}")
            return []
            
    def get_historical_data(self, symbol: str) -> pd.DataFrame:
        """Get historical OHLCV data with efficient caching"""
        
        # Check memory cache
        if symbol in self.cache:
            logger.info(f"Using cached data for {symbol}")
            return self.cache[symbol]
            
        # Check disk cache
        cache_file = config.CACHE_DIR / f"{symbol}.parquet"
        if cache_file.exists():
            df = pd.read_parquet(cache_file)
            last_date = df.index[-1]
            
            if datetime.now() - last_date < timedelta(days=1):
                self.cache[symbol] = df
                return df
                
            # Update existing data
            new_data = self._fetch_missing_data(symbol, last_date)
            if new_data is not None:
                df = pd.concat([df, new_data])
                df = df[~df.index.duplicated(keep='last')]
                df.sort_index(inplace=True)
                
                # Cache updated data
                df.to_parquet(cache_file)
                self.cache[symbol] = df
                return df
                
        # Fetch full history
        df = self._fetch_full_history(symbol)
        if df is not None:
            df.to_parquet(cache_file)
            self.cache[symbol] = df
            
        return df
        
    def _fetch_full_history(self, symbol: str) -> pd.DataFrame:
        """Fetch complete historical data"""
        try:
            klines = self.client.get_historical_klines(
                symbol,
                Client.KLINE_INTERVAL_1DAY,
                limit=1000
            )
            
            if not klines:
                logger.warning(f"No data found for {symbol}")
                return None
                
            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_volume', 'trades', 'taker_buy_volume',
                'taker_buy_quote_volume', 'ignored'
            ])
            
            # Process data
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            # Convert types
            for col in ['open', 'high', 'low', 'close', 'volume', 'quote_volume']:
                df[col] = df[col].astype(float)
            df['trades'] = df['trades'].astype(int)
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching history for {symbol}: {e}")
            return None
            
    def _fetch_missing_data(self, symbol: str, last_date: datetime) -> pd.DataFrame:
        """Fetch only missing data since last update"""
        try:
            start_ms = int((last_date + timedelta(days=1)).timestamp() * 1000)
            
            klines = self.client.get_historical_klines(
                symbol,
                Client.KLINE_INTERVAL_1DAY,
                start_str=str(start_ms),
                limit=1000
            )
            
            if not klines:
                return None
                
            return self._process_klines(klines)
            
        except Exception as e:
            logger.error(f"Error fetching updates for {symbol}: {e}")
            return None
            
    def _process_klines(self, klines: list) -> pd.DataFrame:
        """Convert raw klines to DataFrame"""
        df = pd.DataFrame(klines, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_volume', 'trades', 'taker_buy_volume',
            'taker_buy_quote_volume', 'ignored'
        ])
        
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        
        for col in ['open', 'high', 'low', 'close', 'volume', 'quote_volume']:
            df[col] = df[col].astype(float)
        df['trades'] = df['trades'].astype(int)
        
        return df
    
