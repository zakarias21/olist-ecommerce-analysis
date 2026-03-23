# Brazilian E-Commerce Analysis (Olist Dataset) | 2016–2018
> Analyzing **100K+ orders** across sales overview, time, products, payments, customer satisfaction, and delivery performance to uncover what drives revenue, loyalty, and operational efficiency on Brazil's leading e-commerce platform.

## Executive Summary

Olist, Brazil's largest e-commerce marketplace, grew revenue **20× in two years**.
This analysis answers three questions every ops or commercial leader cares about:

**1. Where is the money coming from?**
Growth was entirely volume-driven — the platform acquired more customers, not bigger
spenders. Average order value held flat at ~BRL 160 all year. The business scaled
because the funnel widened, not because prices went up.

**2. What is the single biggest risk to that growth?**
Late deliveries. Customers who waited 21+ days gave 1-star reviews at 3× the rate
of customers who waited 10 days. One bad month of logistics erases months of
satisfaction gains. This was proven in 2018 Q1, when the late rate hit 19% —
and review scores dropped immediately.

**3. Where should the business invest next?**
`health_beauty` is the only category ranking top-2 in both revenue and units sold.
Every other top category requires a trade-off between revenue per unit and volume.
This one doesn't.

> **The single metric that predicts everything else: Monthly Late Delivery Rate.**
> Keep it below 7%. Above 10%, trigger a logistics audit immediately.

**[View the full no-code report →](file:///Users/napi/Analysis-portfolio/olist-ecommerce-analysis/olist-ecommerce-analysis/reports/olist_report.html)**
*(Charts and insights only — no code visible)*

---

## Key Findings at a Glance

| Finding | What It Means |
|---|---|
| **BRL 15.4M revenue, 96K orders** | Realized revenue from delivered orders only — cancelled orders excluded |
| **AOV stable at BRL 160 year-round** | Customers don't spend more in peak months — volume is the only growth lever |
| **92% of orders arrive early** | Deliberate strategy: Olist sets estimates 12 days beyond actual delivery time |
| **Score-1 customers waited 2× longer** | 20.85 days avg for 1-star vs 10.22 days for 5-star — delivery time is the predictor |
| **2018 Q1: late rate hit 19%** | The only structural failure in 2 years — satisfaction damage lasted 3+ months |
| **health_beauty leads both dimensions** | #1 in revenue (BRL 1.26M) AND #2 in volume (9,670 units) — uniquely balanced |
| **Credit card = 77% of transactions** | Boleto (bank slip) holds a structurally significant 20% — not a marginal segment |

---

## Project Structure

```
olist-ecommerce-analysis/
│
├── README.md                       # You are here
├── requirements.txt                # Pinned dependencies
├── .gitignore                      # Excludes data/ and cache files
│
├── data/
│   ├── raw/                        # Original 9 CSVs (git-ignored)
│   └── processed/                  # Parquet cache files (git-ignored)
│
├── notebooks/
│   └── olist_analysis.ipynb        # Main analysis — narrative + visualizations
│
├── reports/
│   └── olist_report.html           # No-code HTML export for non-technical viewers
│
├── src/
│   ├── __init__.py
│   ├── data_processing.py          # Core module: load, clean, feature engineer
│   └── visualizations.py          # Reusable plotting functions
│
└── tests/
    └── test_data_processing.py     # Sanity checks on core functions
```
---

## Module Design
*This section is for technical viewers.*

All data loading, cleaning, and feature engineering is isolated in `src/data_processing.py`.
The notebook imports clean, analysis-ready DataFrames — no raw transformation logic
in the narrative layer.

### `load_and_clean_all(base_path) → dict[str, pd.DataFrame]`

Loads all 9 Olist CSV files and applies every cleaning step in a single call.

What it does per table:

| Table | Nulls | Duplicates | Type Fixes | Notes |
|---|---|---|---|---|
| customers | None found | Defensive drop | zip prefix → object | Zip is categorical, not numeric |
| orders | Date cols → NaT intentional | Defensive drop | 5 cols → datetime | NaT = undelivered, filtered downstream |
| order_items | None found | Defensive drop | shipping_limit_date → datetime, item_id → object | |
| payments | None found | Defensive drop | — | |
| reviews | Comment cols dropped (high null, unused) | Defensive drop | 2 cols → datetime | Null scores dropped |
| products | category fillna('unknown'), count cols fillna(0) | Defensive drop | count cols → int | 2 rows dropped (missing dimensions) |
| sellers | None found | Defensive drop | city → lowercase | |
| geolocation | None found | Handled in aggregate_geolocation() | — | Dedup deferred — 1M rows processed once |
| category_translation | None found | Defensive drop | name cols → lowercase/strip | |

### `build_delivery_metrics(orders) → pd.DataFrame`

Adds five columns to the orders DataFrame in one call. Consolidates logic that was
previously scattered across 5 separate notebook cells.

```
delivery_days           actual days from purchase → delivered to customer
estimated_delivery_days days from purchase → promised delivery date
delay_days              actual minus estimated (negative = early, positive = late)
delivery_status         'Early' / 'On-Time' / 'Late' label
purchase_month          Period column (YYYY-MM) for monthly trend analysis
```

### `aggregate_geolocation(geo, cache_path=None) → pd.DataFrame`

Deduplicates the 1M+ row geolocation table and aggregates to zip-prefix level.

Performance profile:

```
First run  : ~2s  (deduplicates 261,831 rows → groups 738,332 → 19,015 zip prefixes)
Cached runs: ~0.1s (reads data/processed/geo_agg.parquet directly)
```

The `cache_path` parameter is optional. Pass `'data/processed/geo_agg.parquet'`
to enable caching. The cache is git-ignored and lives locally only.

---

## Analysis Sections

| Section | Business Question | Method |
|---|---|---|
| **I. Sales Overview** | Is revenue elite-driven or volume-driven? | Pareto curve, AOV distribution |
| **II. Time Analysis** | Is growth sustainable or concentrated in volatile months? | Monthly revenue trend, seasonality bar chart |
| **III. Product Analysis** | Which categories deserve investment? | Revenue vs volume dual ranking |
| **IV. Payment Analysis** | Does payment infrastructure serve all segments? | Payment type distribution, installment frequency |
| **V. Customer Satisfaction** | What operational variable predicts a 1-star review? | Delivery bucket heatmap, violin plot by score |
| **VI. Delivery Performance** | Is reliability improving over time? | Monthly late delivery rate trend line |

---

## Data

**Source:** [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — Kaggle

**Period:** September 2016 – August 2018

| Table | Raw Rows | After Cleaning |
|---|---|---|
| olist_orders | 99,441 | 99,441 |
| olist_order_items | 112,650 | 112,650 |
| olist_order_payments | 103,886 | 103,886 |
| olist_order_reviews | 99,224 | 99,224 |
| olist_products | 32,951 | 32,949 |
| olist_customers | 99,441 | 99,441 |
| olist_sellers | 3,095 | 3,095 |
| olist_geolocation | 1,000,163 | 19,015 *(aggregated to zip prefix)* |
| product_category_translation | 71 | 71 |

---

## Operational Recommendations

**1. Monitor the monthly late delivery rate as a leading indicator**
Below 7% → normal. 7–10% → elevated risk, investigate. Above 10% → immediate
logistics audit. The 2018 Q1 data shows two consecutive months above threshold
produce satisfaction damage that takes 3+ months to reverse.

**2. Prioritize health_beauty for inventory and seller investment**
It is the only category that does not require a trade-off between revenue per unit
and total units sold. All other top categories optimize for one at the expense of the other.

**3. Retain boleto as a first-class payment option**
20% of transactions (19,783 orders) use boleto — Brazil-specific bank-slip payment
for unbanked customers. Deprioritizing it would structurally exclude 1 in 5 customers.

**4. Promote 8 and 10-installment credit card plans for high-value categories**
The spike in 8 and 10-installment usage is not organic behavior — it reflects
structured plan offerings. Extending these to watches_gifts and computers_accessories
could lift AOV without requiring price increases.

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.11 | Core language |
| pandas 2.1 | Data manipulation |
| matplotlib / seaborn | Visualization |
| pyarrow | Parquet caching for geolocation |
| JupyterLab | Notebook environment |
| Git | Version control |

---

*Analysis by [zakarias musa] | [LinkedIn](www.linkedin.com/in/zakariasmusa2193) | [GitHub](https://github.com/zakarias-21)*
