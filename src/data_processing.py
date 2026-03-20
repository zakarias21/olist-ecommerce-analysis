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

    # ── 1. LOAD ───────────────────────────────────────────────────────────────
    customers            = pd.read_csv(base_path + "olist_customers_dataset.csv")
    orders               = pd.read_csv(base_path + "olist_orders_dataset.csv")
    order_items          = pd.read_csv(base_path + "olist_order_items_dataset.csv")
    payments             = pd.read_csv(base_path + "olist_order_payments_dataset.csv")
    reviews              = pd.read_csv(base_path + "olist_order_reviews_dataset.csv")
    products             = pd.read_csv(base_path + "olist_products_dataset.csv")
    sellers              = pd.read_csv(base_path + "olist_sellers_dataset.csv")
    geolocation          = pd.read_csv(base_path + "olist_geolocation_dataset.csv")
    category_translation = pd.read_csv(base_path + "product_category_name_translation.csv")

    # ── 2. CUSTOMERS ──────────────────────────────────────────────────────────
    # Notebook confirmed: no nulls, no duplicates.
    # Defensive guard added for future batch data.
    customers = customers.drop_duplicates()
    # Zip prefix is categorical — no math will ever be done on it
    customers['customer_zip_code_prefix'] = (
        customers['customer_zip_code_prefix'].astype('object')
    )

    # ── 3. ORDERS ─────────────────────────────────────────────────────────────
    # Notebook confirmed: no duplicates.
    # Missing delivery dates are INTENTIONAL — they are undelivered orders.
    # NaT rows are excluded naturally when computing delivery metrics downstream.
    # We do NOT impute them.
    orders = orders.drop_duplicates()
    date_cols = [
        'order_purchase_timestamp',
        'order_delivered_customer_date',
        'order_delivered_carrier_date',
        'order_approved_at',
        'order_estimated_delivery_date'
    ]
    orders[date_cols] = orders[date_cols].apply(
        lambda col: pd.to_datetime(col, errors='coerce')
    )

    # ── 4. ORDER ITEMS ────────────────────────────────────────────────────────
    # Notebook confirmed: no nulls, no duplicates.
    # Defensive guard added for future batch data.
    order_items = order_items.drop_duplicates()
    order_items['shipping_limit_date'] = pd.to_datetime(
        order_items['shipping_limit_date'], errors='coerce'
    )
    # Item ID is a label, not a quantity — treat as categorical
    order_items['order_item_id'] = order_items['order_item_id'].astype('object')

    # ── 5. PAYMENTS ───────────────────────────────────────────────────────────
    # Notebook confirmed: no nulls, no duplicates.
    # Defensive guard added for future batch data.
    payments = payments.drop_duplicates()

    # ── 6. REVIEWS ────────────────────────────────────────────────────────────
    # Notebook confirmed: no duplicates.
    # Comment columns have too many nulls and are not used in analysis — dropped.
    # review_score nulls: none found, but defensively drop any that appear
    # since a null score cannot be analyzed.
    reviews = reviews.drop_duplicates()
    reviews.drop(
        columns=['review_comment_title', 'review_comment_message'],
        inplace=True
    )
    reviews = reviews.dropna(subset=['review_score'])
    reviews[['review_creation_date', 'review_answer_timestamp']] = (
        reviews[['review_creation_date', 'review_answer_timestamp']]
        .apply(pd.to_datetime, errors='coerce')
    )

    # ── 7. PRODUCTS ───────────────────────────────────────────────────────────
    # Fix typos in original column names
    products = products.drop_duplicates()
    products = products.rename(columns={
        'product_name_lenght':        'product_name_length',
        'product_description_lenght': 'product_description_length'
    })
    # Preserve revenue rows — 'unknown' category beats a dropped row
    products['product_category_name'] = (
        products['product_category_name'].fillna('unknown')
    )
    # Counts cannot be decimals — fill nulls with 0 before casting
    count_cols = [
        'product_name_length',
        'product_description_length',
        'product_photos_qty'
    ]
    products[count_cols] = products[count_cols].fillna(0).astype(int)

    # Only 2 rows affected — dropping avoids unrealistic imputation
    # Cannot estimate weight/dimensions from other columns
    products = products.dropna(subset=[
        'product_weight_g',
        'product_length_cm',
        'product_height_cm',
        'product_width_cm'
    ])
    # Standardize for clean merges with category_translation
    products['product_category_name'] = (
        products['product_category_name'].str.strip().str.lower()
    )
    # Feature engineering: volume for freight/logistics analysis
    products['product_volume_cm3'] = (
        products['product_length_cm'] *
        products['product_height_cm'] *
        products['product_width_cm']
    )

    # ── 8. SELLERS ────────────────────────────────────────────────────────────
    # Notebook confirmed: no nulls, seller_id is unique.
    # Defensive guard added for future batch data.
    sellers = sellers.drop_duplicates()
    sellers['seller_city'] = sellers['seller_city'].str.strip().str.lower()

    # ── 9. GEOLOCATION ────────────────────────────────────────────────────────
    # NOTE: geolocation has known duplicate rows (confirmed in notebook).
    # Deduplication happens inside aggregate_geolocation() — not here —
    # because the raw table is only ever used through that function.
    # No nulls found in notebook.

    # ── 10. CATEGORY TRANSLATION ──────────────────────────────────────────────
    # Notebook confirmed: no nulls, no duplicates after standardizing.
    category_translation = category_translation.drop_duplicates()
    category_translation['product_category_name'] = (
        category_translation['product_category_name'].str.strip().str.lower()
    )
    category_translation['product_category_name_english'] = (
        category_translation['product_category_name_english'].str.strip()
    )

    # ── 11. RETURN ALL TABLES ─────────────────────────────────────────────────
    print("✅ All tables loaded and cleaned successfully.")
    print(f"   customers:            {customers.shape}")
    print(f"   orders:               {orders.shape}")
    print(f"   order_items:          {order_items.shape}")
    print(f"   payments:             {payments.shape}")
    print(f"   reviews:              {reviews.shape}")
    print(f"   products:             {products.shape}")
    print(f"   sellers:              {sellers.shape}")
    print(f"   geolocation:          {geolocation.shape}")
    print(f"   category_translation: {category_translation.shape}")

    return {
        'customers':            customers,
        'orders':               orders,
        'order_items':          order_items,
        'payments':             payments,
        'reviews':              reviews,
        'products':             products,
        'sellers':              sellers,
        'geolocation':          geolocation,
        'category_translation': category_translation,
    }


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

    Args:
        geo        : Raw geolocation DataFrame (1M+ rows).
        cache_path : Optional path to read/write a parquet cache.

    Returns:
        Aggregated DataFrame with one row per zip_code_prefix.
    """
    pass

def build_delivery_metrics(orders: pd.DataFrame) -> pd.DataFrame:
    """
    Adds five delivery columns to the orders DataFrame:
        - delivery_days          : actual days from purchase to delivery
        - estimated_delivery_days: estimated days from purchase to delivery
        - delay_days             : actual minus estimated (negative = early)
        - delivery_status        : 'Early', 'On-Time', or 'Late'
        - purchase_month         : period column (YYYY-MM) for monthly trend analysis

    Only operates on delivered orders — non-delivered rows get NaN in all columns.

    Args:
        orders: Cleaned orders DataFrame with datetime columns already parsed
                by load_and_clean_all().

    Returns:
        The same DataFrame with five new columns added.
    """

    # ── 1. WORK ON A COPY ─────────────────────────────────────────────────────
    # We never mutate the input — caller keeps their original df intact
    df = orders.copy()

    # ── 2. DELIVERY DURATION METRICS ──────────────────────────────────────────
    # Actual delivery time: purchase → delivered to customer
    df['delivery_days'] = (
        df['order_delivered_customer_date'] -
        df['order_purchase_timestamp']
    ).dt.days

    # Estimated delivery time: purchase → what was promised
    df['estimated_delivery_days'] = (
        df['order_estimated_delivery_date'] -
        df['order_purchase_timestamp']
    ).dt.days

    # Delay: positive = late, negative = early, zero = exactly on time
    # This is the single most important operational KPI in the dataset
    df['delay_days'] = (
        df['order_delivered_customer_date'] -
        df['order_estimated_delivery_date']
    ).dt.days

    # ── 3. DELIVERY STATUS LABEL ──────────────────────────────────────────────
    # Only classify rows that have a computed delay_days value
    # (i.e. delivered orders) — undelivered rows stay NaN
    df['delivery_status'] = df['delay_days'].apply(
        lambda x: 'Late'    if x > 0  else
                  'On-Time' if x == 0 else
                  'Early'   if x < 0  else
                  None  # NaN delay_days → undelivered
    )

    # ── 4. PURCHASE MONTH ─────────────────────────────────────────────────────
    # Period column for monthly late delivery rate trend (Section VI in notebook)
    df['purchase_month'] = (
        pd.to_datetime(df['order_purchase_timestamp'])
        .dt.to_period('M')
    )

    # ── 5. SUMMARY PRINT ──────────────────────────────────────────────────────
    delivered = df[df['order_status'] == 'delivered']
    late_pct   = (delivered['delay_days'] > 0).mean() * 100
    avg_days   = delivered['delivery_days'].mean()

    print("✅ Delivery metrics built successfully.")
    print(f"   Delivered orders   : {len(delivered):,}")
    print(f"   Avg delivery time  : {avg_days:.2f} days")
    print(f"   Late delivery rate : {late_pct:.2f}%")

    return df

def aggregate_geolocation(
    geo: pd.DataFrame,
    cache_path: str | None = None
) -> pd.DataFrame:
    """
    Deduplicates and aggregates raw geolocation to zip-prefix level.

    Steps:
        1. Check for a parquet cache — if it exists, load and return it instantly
        2. Normalize city (lowercase) and state (uppercase) strings
        3. Drop exact duplicate rows from the raw 1M+ row table
        4. Group by zip prefix → mean lat, mean lng, first city, first state
        5. Save the result to parquet cache for all future runs

    Args:
        geo        : Raw geolocation DataFrame (1M+ rows) from load_and_clean_all().
        cache_path : Optional path to read/write a parquet cache.
                     Recommended: 'data/processed/geo_agg.parquet'
                     - If file exists  → skips all processing, loads in ~0.1s
                     - If file missing → builds from raw, saves for next time

    Returns:
        Aggregated DataFrame (~19k rows) with one row per zip_code_prefix.
    """

    import os

    # ── 1. CACHE CHECK ────────────────────────────────────────────────────────
    # If a cache file exists, skip the entire 1M-row pipeline
    # This turns a ~30s operation into a ~0.1s load on every run after the first
    if cache_path and os.path.exists(cache_path):
        print(f"⚡ Loading geo_agg from cache: {cache_path}")
        geo_agg = pd.read_parquet(cache_path)
        print(f"   Loaded {len(geo_agg):,} zip prefixes from cache.")
        return geo_agg

    print("🔄 Building geo_agg from raw data (1M+ rows) — this runs once...")

    # ── 2. NORMALIZE ──────────────────────────────────────────────────────────
    # Lowercase city: prevents 'Sao Paulo' vs 'sao paulo' grouping as two cities
    # Uppercase state: standard BR state code format (SP, RJ, MG...)
    geo = geo.copy()
    geo['geolocation_city'] = (
        geo['geolocation_city']
        .str.strip()
        .str.lower()
    )
    geo['geolocation_state'] = (
        geo['geolocation_state']
        .str.strip()
        .str.upper()
    )

    # ── 3. DEDUPLICATE ────────────────────────────────────────────────────────
    # The raw table has thousands of identical rows — confirmed in notebook.
    # Dropping them before groupby significantly reduces memory overhead.
    before = len(geo)
    geo = geo.drop_duplicates()
    after  = len(geo)
    print(f"   Duplicates removed : {before - after:,} rows")
    print(f"   Rows remaining     : {after:,}")

    # ── 4. AGGREGATE BY ZIP PREFIX ────────────────────────────────────────────
    # Coordinates vary slightly within the same prefix (different streets).
    # Mean lat/lng gives a stable centroid for the prefix — good enough for
    # regional analysis and merging with customers/sellers tables.
    geo_agg = (
        geo
        .groupby('geolocation_zip_code_prefix', as_index=False)
        .agg({
            'geolocation_lat':  'mean',
            'geolocation_lng':  'mean',
            'geolocation_city': 'first',
            'geolocation_state':'first'
        })
    )

    print(f"   Unique zip prefixes : {len(geo_agg):,}")

    # ── 5. SAVE TO PARQUET CACHE ──────────────────────────────────────────────
    # All future runs skip straight to step 1 — zero reprocessing
    if cache_path:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        geo_agg.to_parquet(cache_path, index=False)
        print(f"   ✅ Cache saved to: {cache_path}")

    print("✅ geo_agg built successfully.")
    return geo_agg