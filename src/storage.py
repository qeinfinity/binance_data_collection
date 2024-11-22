# src/storage.py
from pathlib import Path
import pandas as pd
from datetime import datetime
import time
import logging
from . import config

logger = logging.getLogger(__name__)

class DataStorage:
    def __init__(self, root_dir: Path = config.CACHE_DIR):
        self.root_dir = root_dir
        self.backup_dir = config.BACKUP_DIR
        self.version = config.DATA_VERSION
        
    def store(self, symbol: str, df: pd.DataFrame) -> bool:
        try:
            filename = f"{symbol}_v{self.version}.parquet"
            filepath = self.root_dir / filename
            
            # Backup existing file
            if filepath.exists():
                backup_name = f"{symbol}_v{self.version}_{int(time.time())}.parquet"
                filepath.rename(self.backup_dir / backup_name)
            
            # Add metadata
            df.attrs = {
                'version': self.version,
                'timestamp': datetime.now().isoformat(),
                'rows': len(df),
                'checksum': pd.util.hash_pandas_object(df).sum()
            }
            
            df.to_parquet(filepath)
            return True
            
        except Exception as e:
            logger.error(f"Error storing data for {symbol}: {e}")
            return False
            
    def load(self, symbol: str) -> pd.DataFrame:
        try:
            filename = f"{symbol}_v{self.version}.parquet"
            filepath = self.root_dir / filename
            
            if not filepath.exists():
                return None
                
            df = pd.read_parquet(filepath)
            return df
            
        except Exception as e:
            logger.error(f"Error loading data for {symbol}: {e}")
            return None
            
    def needs_update(self, symbol: str) -> bool:
        df = self.load(symbol)
        if df is None:
            return True
            
        if 'timestamp' not in df.attrs:
            return True
            
        last_update = datetime.fromisoformat(df.attrs['timestamp'])
        return datetime.now() - last_update > pd.Timedelta(days=1)