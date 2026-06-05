# Brazilian E-Commerce Analysis (Olist Dataset) | 2016–2018

Analyzing 100K+ e-commerce orders across sales, products, payments, customer satisfaction, and delivery performance to identify the key drivers of growth, loyalty, and operational efficiency on Brazil's largest online marketplace.

**Dataset:** [Olist Brazilian E-Commerce Dataset (Kaggle)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

# Executive Summary

Olist grew revenue nearly 20× between 2016 and 2018. This analysis focuses on three business questions that directly impact commercial and operational performance.

## 1. Where Is Growth Coming From?

Growth was driven almost entirely by increasing order volume rather than higher customer spending.

- Revenue increased nearly 20× over the study period.
- Average Order Value (AOV) remained stable at approximately BRL 160 throughout the year.
- The platform scaled by acquiring more customers and processing more orders rather than increasing basket size.

## 2. What Is the Biggest Risk to Continued Growth?

Delivery performance emerged as the strongest operational driver of customer satisfaction.

- Customers who gave 1-star reviews waited an average of 20.85 days.
- Customers who gave 5-star reviews waited only 10.22 days.
- During Q1 2018, the late-delivery rate reached 19%, triggering an immediate decline in customer satisfaction.

## 3. Where Should the Business Invest?

Among all product categories, **health_beauty** demonstrated the strongest balance of scale and profitability.

- #1 category by revenue (BRL 1.26M)
- #2 category by units sold (9,670)

Unlike other leading categories, it did not require a trade-off between revenue and sales volume.

### Most Important KPI

**Monthly Late Delivery Rate**

Recommended thresholds:

- Below 7% → Healthy
- 7–10% → Elevated Risk
- Above 10% → Immediate Logistics Review

---

# Tech Stack

| Tool | Purpose |
|--------|--------|
| Python 3.11 | Data Analysis |
| Pandas | Data Cleaning & Transformation |
| Matplotlib | Visualization |
| Seaborn | Statistical Visualization |
| PyArrow | Parquet Caching |
| Jupyter Notebook | Analysis Environment |
| Git & GitHub | Version Control |

---

# Dataset Overview

## Analysis Period

September 2016 – August 2018

## Tables Used

| Table | Raw Rows | After Cleaning |
|---------|---------:|---------:|
| Orders | 99,441 | 99,441 |
| Order Items | 112,650 | 112,650 |
| Payments | 103,886 | 103,886 |
| Reviews | 99,224 | 99,224 |
| Products | 32,951 | 32,949 |
| Customers | 99,441 | 99,441 |
| Sellers | 3,095 | 3,095 |
| Geolocation | 1,000,163 | 19,015 (aggregated) |
| Category Translation | 71 | 71 |

---

# Analysis Sections

| Section | Business Question | Method |
|----------|------------------|----------|
| Sales Overview | Is growth volume-driven or value-driven? | Pareto analysis, AOV distribution |
| Time Analysis | Is growth sustainable over time? | Revenue trends and seasonality |
| Product Analysis | Which categories deserve investment? | Revenue and volume rankings |
| Payment Analysis | How do customers prefer to pay? | Payment type and installment analysis |
| Customer Satisfaction | What predicts poor reviews? | Delivery-time segmentation |
| Delivery Performance | Is reliability improving? | Monthly late-delivery trends |

---

# Key Findings

| Finding | Business Meaning |
|----------|----------------|
| BRL 15.4M revenue from 96K delivered orders | Revenue calculations exclude cancelled orders |
| Average Order Value remained near BRL 160 | Growth came from volume, not larger purchases |
| 92% of orders arrived early | Delivery estimates were intentionally conservative |
| 1-star customers waited twice as long as 5-star customers | Delivery speed strongly influences satisfaction |
| Late-delivery rate peaked at 19% in Q1 2018 | Largest operational disruption in the dataset |
| health_beauty ranked top in both revenue and volume | Strong candidate for inventory and marketing investment |
| Credit cards represented 77% of transactions | Dominant payment method |
| Boleto accounted for approximately 20% of orders | Critical payment option for customer accessibility |

---

# Repository Structure

```text
olist-ecommerce-analysis/

├── README.md
├── requirements.txt
├── .gitignore
│
├── data/
│   ├── raw/
│   └── processed/
│
├── notebooks/
│   └── olist_analysis.ipynb
│
├── src/
│   ├── __init__.py
│   ├── data_processing.py
│   └── visualizations.py
│
└── tests/
    └── test_data_processing.py
```

---

# Data Processing Pipeline

The project separates data preparation from analysis to improve maintainability and reproducibility.

## load_and_clean_all()

Loads and cleans all Olist tables through a single function.

Processing includes:

- Missing-value handling
- Duplicate removal
- Datetime conversion
- Category standardization
- Feature preparation

## build_delivery_metrics()

Creates delivery-related analytical features:

- delivery_days
- estimated_delivery_days
- delay_days
- delivery_status
- purchase_month

## aggregate_geolocation()

Aggregates more than one million geolocation records to ZIP-prefix level and supports local Parquet caching.

Performance:

- First execution: ~2 seconds
- Cached execution: ~0.1 seconds

---

# Business Recommendations

## 1. Monitor Late Delivery Rate as a Core KPI

Delivery performance is the strongest leading indicator of customer satisfaction.

Recommended thresholds:

- Below 7% → Normal
- 7–10% → Investigate
- Above 10% → Immediate logistics audit

The Q1 2018 disruption demonstrates how quickly customer experience deteriorates when delays increase.

## 2. Prioritize Health & Beauty Category Growth

health_beauty was the only major category performing strongly in both revenue generation and sales volume.

Potential actions:

- Increase inventory availability
- Expand seller recruitment
- Allocate additional marketing budget

## 3. Preserve Boleto as a Strategic Payment Option

Nearly 20% of orders used boleto payments.

Removing or deprioritizing this payment channel would risk excluding a significant customer segment.

## 4. Expand Installment-Based Purchasing

Installment plans showed strong adoption for higher-value purchases.

Extending installment offerings to categories such as:

- watches_gifts
- computers_accessories

could increase Average Order Value without increasing listed prices.

---

# Skills Demonstrated

- Data Cleaning
- Exploratory Data Analysis (EDA)
- Feature Engineering
- Customer Analytics
- Product Analytics
- Logistics & Operations Analysis
- Business Intelligence
- Data Visualization
- Performance Optimization
- Repository Organization
- Analytical Storytelling

---

# Author

**Zakarias Musa**

Data Analyst & BI Developer
