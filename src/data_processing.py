# src/data_processing.py
"""
Data loading, cleaning, and feature engineering for the Olist dataset.
All raw → clean transformations live here, keeping the notebook thin.
"""

import pandas as pd


def load_and_clean_all(base_path: str) -> dict[str, pd.DataFrame]:
    """
    Loads and cleans all 9 Olist source tables from base_path.

    Args:
        base_path: Path to the folder containing the raw CSV files.
                   Must end with a trailing slash e.g. 'data/raw/'

    Returns:
        A dict keyed by table name:
        {
            'customers', 'orders', 'order_items',
            'payments',  'reviews', 'products',
            'sellers',   'geolocation', 'category_translation'
        }
    """
    pass


def build_delivery_metrics(orders: pd.DataFrame) -> pd.DataFrame:
    """
    Adds three delivery columns to the orders DataFrame:
        - delivery_days   : actual days from purchase to delivery
        - delay_days      : actual minus estimated (negative = early)
        - delivery_status : 'Early', 'On-Time', or 'Late'

    Args:
        orders: Cleaned orders DataFrame with datetime columns already parsed.

    Returns:
        The same DataFrame with three new columns added.
    """
    pass


def aggregate_geolocation(
    geo: pd.DataFrame,
    cache_path: str | None = None
) -> pd.DataFrame:
    """
    Deduplicates and aggregates raw geolocation to zip-prefix level.

    Steps:
        1. Normalize city/state strings (strip, lowercase)
        2. Drop exact duplicate rows
        3. Group by zip prefix → mean lat, mean lon

    Args:
        geo        : Raw geolocation DataFrame (1M+ rows).
        cache_path : Optional path to read/write a parquet cache
                     e.g. 'data/processed/geo_agg.parquet'
                     If file exists → loads it. If not → builds and saves it.

    Returns:
        Aggregated DataFrame with one row per zip_code_prefix.
    """
    pass