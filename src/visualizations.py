# src/visualizations.py
"""
Reusable plotting functions for the Olist analysis.
Each function takes a clean DataFrame and returns a matplotlib Figure.
"""

import pandas as pd
import matplotlib.pyplot as plt


def plot_monthly_revenue(monthly_df: pd.DataFrame) -> plt.Figure:
    """
    Line chart of monthly revenue trend.

    Args:
        monthly_df: DataFrame with columns ['order_month', 'payment_value']

    Returns:
        matplotlib Figure object.
    """
    pass


def plot_late_delivery_rate(delivery_df: pd.DataFrame) -> plt.Figure:
    """
    Line chart of monthly late delivery rate over time.

    Args:
        delivery_df: DataFrame with delivery_status and purchase month columns.

    Returns:
        matplotlib Figure object.
    """
    pass


def plot_review_score_distribution(reviews_df: pd.DataFrame) -> plt.Figure:
    """
    Bar chart of review score counts (1–5).

    Args:
        reviews_df: DataFrame with a 'review_score' column.

    Returns:
        matplotlib Figure object.
    """
    pass