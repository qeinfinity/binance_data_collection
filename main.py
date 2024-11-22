# main.py
from src.data import DataManager
from src import utils
import logging
from typing import Dict
import pandas as pd


logger = utils.setup_logging()

def verify_data_quality(data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Dict]:
    """Verify quality of collected data"""
    stats = {}
    for symbol, df in data_dict.items():
        stats[symbol] = {
            'days': len(df),
            'missing_days': len(pd.date_range(df.index[0], df.index[-1])) - len(df),
            'zero_volume_days': (df['volume'] == 0).sum(),
            'zero_trades_days': (df['trades'] == 0).sum(),
            'date_range': f"{df.index[0]} to {df.index[-1]}"
        }
    return stats

def main():
    # Initialize data manager
    dm = DataManager()
    
    # Get tradeable symbols
    symbols = dm.get_tradeable_symbols()
    logger.info(f"Found {len(symbols)} tradeable symbols")
    
    # Fetch historical data for each symbol
    valid_data = {}
    for symbol in symbols:
        df = dm.get_historical_data(symbol)
        if utils.validate_data(df):
            valid_data[symbol] = df
            logger.info(f"Valid data collected for {symbol}")

    
    logger.info(f"Successfully collected data for {len(valid_data)} symbols")

    # Verify data quality
    quality_stats = verify_data_quality(valid_data)
    
    # Log quality statistics
    logger.info("\nData Quality Report:")
    for symbol, stats in quality_stats.items():
        issues = []
        if stats['missing_days'] > 0:
            issues.append(f"{stats['missing_days']} missing days")
        if stats['zero_volume_days'] > 0:
            issues.append(f"{stats['zero_volume_days']} zero volume days")
        if stats['zero_trades_days'] > 0:
            issues.append(f"{stats['zero_trades_days']} zero trades days")
            
        if issues:
            logger.warning(f"{symbol}: {', '.join(issues)}")
        else:
            logger.info(f"{symbol}: Clean data, {stats['days']} days from {stats['date_range']}")

    return valid_data, quality_stats

if __name__ == "__main__":
    main()