"""
Sales Analyzer Module
=====================

This module provides the SalesAnalyzer class - the core analysis engine for
performing data aggregation, grouping, and analytical operations on sales data
using the pandas API.

Design Philosophy:
-----------------
This module demonstrates proficiency with functional programming paradigms:

1. **Stream-like Operations**: Method chaining for declarative data processing
2. **Lambda Expressions**: Used extensively in apply(), map(), filter()
3. **Data Aggregation**: GroupBy operations with multiple aggregation functions
4. **Functional Filtering**: Predicate-based filtering returning new instances

The design follows these principles:
- Immutability: Filter methods return new instances, not modified originals
- Fluent Interface: Methods can be chained for readable, concise code
- Single Responsibility: Each method performs one specific analysis

Pandas API Features Demonstrated:
--------------------------------
- DataFrame.groupby() - Group data by one or more columns
- DataFrame.agg() - Apply multiple aggregation functions
- DataFrame.apply() - Apply functions along an axis
- DataFrame.pipe() - Chain custom functions
- DataFrame.pivot_table() - Create spreadsheet-style pivot tables
- Series.map() - Element-wise transformation
- Series.nlargest() - Get top N values

Author: [Your Name]
Date: December 2024
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any, Optional, Callable
from pathlib import Path

from src.data_loader import DataLoader


class SalesAnalyzer:
    """
    Core analysis class for sales data using pandas API.
    
    This class provides comprehensive methods for analyzing sales data
    through aggregation, grouping, filtering, and statistical operations.
    All methods use functional programming patterns and the pandas API.
    
    The class is designed with immutability in mind - filter operations
    return new SalesAnalyzer instances rather than modifying the original.
    
    Attributes:
        _df (pd.DataFrame): Internal DataFrame storing the sales data
    
    Example:
        Basic usage:
        >>> analyzer = SalesAnalyzer("data/sales_data.csv")
        >>> print(f"Total Revenue: ${analyzer.total_revenue():,.2f}")
        Total Revenue: $12,787.15
        
        Method chaining (stream-like):
        >>> result = (analyzer
        ...     .filter_by_category("Electronics")
        ...     .filter_by_region("North")
        ...     .total_revenue())
        
        Using lambda predicates:
        >>> bulk_orders = analyzer.filter_by_predicate(
        ...     lambda row: row['quantity'] > 5
        ... )
    """

    def __init__(self, data_source: str | pd.DataFrame):
        """
        Initialize the SalesAnalyzer with data from file or DataFrame.
        
        Args:
            data_source: Either a file path (str) to a CSV file or 
                        an existing pandas DataFrame
        
        Raises:
            FileNotFoundError: If file path doesn't exist
            ValueError: If DataFrame is empty or invalid
            
        Example:
            # From file
            >>> analyzer = SalesAnalyzer("data/sales_data.csv")
            
            # From DataFrame
            >>> df = pd.read_csv("data/sales_data.csv")
            >>> analyzer = SalesAnalyzer(df)
        """
        # Load data based on source type
        if isinstance(data_source, pd.DataFrame):
            self._df = data_source.copy()
        else:
            loader = DataLoader(data_source)
            self._df = loader.load()
        
        # Validate data is not empty
        if self._df.empty:
            raise ValueError("Cannot analyze empty dataset")
        
        # Add computed columns
        self._add_computed_columns()

    def _add_computed_columns(self) -> None:
        """
        Add computed columns to the DataFrame for analysis.
        
        Computed columns:
        - gross_amount: quantity × unit_price
        - discount_amount: gross_amount × discount
        - net_amount: gross_amount - discount_amount
        - month: YYYY-MM format for monthly analysis
        - day_of_week: Day name for weekly patterns
        
        Uses vectorized operations for performance and lambda
        expressions for date formatting.
        """
        # Vectorized calculations (faster than apply for numeric operations)
        self._df['gross_amount'] = self._df['quantity'] * self._df['unit_price']
        self._df['discount_amount'] = self._df['gross_amount'] * self._df['discount']
        self._df['net_amount'] = self._df['gross_amount'] - self._df['discount_amount']
        
        # Date-based columns using lambda expressions
        self._df['month'] = self._df['date'].apply(lambda x: x.strftime('%Y-%m'))
        self._df['day_of_week'] = self._df['date'].apply(lambda x: x.strftime('%A'))

    @property
    def dataframe(self) -> pd.DataFrame:
        """
        Get a copy of the underlying DataFrame.
        
        Returns a copy to maintain immutability - modifications to
        the returned DataFrame won't affect the analyzer.
        
        Returns:
            pd.DataFrame: Copy of the internal DataFrame
        """
        return self._df.copy()

    @property
    def record_count(self) -> int:
        """
        Get the number of records in the dataset.
        
        Returns:
            int: Number of sales records
        """
        return len(self._df)

    # =========================================================================
    # AGGREGATION OPERATIONS
    # These methods demonstrate basic pandas aggregation functions
    # =========================================================================

    def total_revenue(self) -> float:
        """
        Calculate total revenue across all sales.
        
        Uses pandas Series.sum() for efficient aggregation.
        
        Returns:
            float: Sum of all net_amount values
            
        Example:
            >>> analyzer.total_revenue()
            12787.15
        """
        return float(self._df['net_amount'].sum())

    def total_quantity_sold(self) -> int:
        """
        Calculate total quantity of items sold.
        
        Returns:
            int: Sum of all quantity values
        """
        return int(self._df['quantity'].sum())

    def average_order_value(self) -> float:
        """
        Calculate the average order value (AOV).
        
        AOV is a key retail metric representing the average
        amount spent per order.
        
        Returns:
            float: Mean of net_amount values
        """
        return float(self._df['net_amount'].mean())

    def total_discount_given(self) -> float:
        """
        Calculate total discount amount given.
        
        Returns:
            float: Sum of all discount_amount values
        """
        return float(self._df['discount_amount'].sum())

    def unique_customers(self) -> int:
        """
        Count unique customers using pandas nunique().
        
        Returns:
            int: Number of distinct customer_id values
        """
        return int(self._df['customer_id'].nunique())

    def unique_products(self) -> int:
        """
        Count unique products sold.
        
        Returns:
            int: Number of distinct product_id values
        """
        return int(self._df['product_id'].nunique())

    # =========================================================================
    # GROUPBY OPERATIONS
    # These methods demonstrate pandas groupby for data aggregation
    # =========================================================================

    def revenue_by_category(self) -> pd.Series:
        """
        Calculate revenue grouped by product category.
        
        Demonstrates: groupby() + sum() + sort_values()
        
        Returns:
            pd.Series: Category names as index, revenue as values,
                      sorted descending by revenue
            
        Example:
            >>> analyzer.revenue_by_category()
            category
            Electronics        8600.65
            Furniture          3426.36
            Office Supplies     760.14
            Name: net_amount, dtype: float64
        """
        return (
            self._df
            .groupby('category')['net_amount']
            .sum()
            .sort_values(ascending=False)
        )

    def revenue_by_region(self) -> pd.Series:
        """
        Calculate revenue grouped by geographic region.
        
        Returns:
            pd.Series: Region names as index, revenue as values
        """
        return (
            self._df
            .groupby('region')['net_amount']
            .sum()
            .sort_values(ascending=False)
        )

    def revenue_by_payment_method(self) -> pd.Series:
        """
        Calculate revenue grouped by payment method.
        
        Returns:
            pd.Series: Payment methods as index, revenue as values
        """
        return (
            self._df
            .groupby('payment_method')['net_amount']
            .sum()
            .sort_values(ascending=False)
        )

    def monthly_revenue(self) -> pd.Series:
        """
        Calculate revenue trend by month.
        
        Uses the pre-computed 'month' column (YYYY-MM format)
        for grouping, ensuring chronological sorting.
        
        Returns:
            pd.Series: Month (YYYY-MM) as index, revenue as values
        """
        return (
            self._df
            .groupby('month')['net_amount']
            .sum()
            .sort_index()
        )

    def order_count_by_category(self) -> pd.Series:
        """
        Count orders by category using groupby().size().
        
        Returns:
            pd.Series: Category names as index, order counts as values
        """
        return (
            self._df
            .groupby('category')
            .size()
            .sort_values(ascending=False)
        )

    def quantity_sold_by_category(self) -> pd.Series:
        """
        Calculate total quantity sold by category.
        
        Returns:
            pd.Series: Category names as index, quantities as values
        """
        return (
            self._df
            .groupby('category')['quantity']
            .sum()
            .sort_values(ascending=False)
        )

    # =========================================================================
    # MULTI-AGGREGATION OPERATIONS
    # These methods demonstrate advanced groupby with multiple aggregations
    # =========================================================================

    def category_region_pivot(self) -> pd.DataFrame:
        """
        Create a pivot table of revenue by category and region.
        
        Demonstrates: pivot_table() for cross-tabulation
        
        Returns:
            pd.DataFrame: Categories as rows, regions as columns,
                         revenue values in cells
            
        Example:
            >>> analyzer.category_region_pivot()
            region              East    North    South     West
            category
            Electronics      1594.94  2556.41  3055.90  1393.40
            Furniture         329.47   520.97  1403.96  1171.95
            Office Supplies   254.82   346.84   158.48     0.00
        """
        return pd.pivot_table(
            self._df,
            values='net_amount',
            index='category',
            columns='region',
            aggfunc='sum',
            fill_value=0
        )

    def category_statistics(self) -> pd.DataFrame:
        """
        Get comprehensive statistics by category.
        
        Demonstrates: groupby().agg() with multiple named aggregations
        
        Returns:
            pd.DataFrame: Multi-column statistics per category
        """
        return (
            self._df
            .groupby('category')
            .agg(
                order_count=('order_id', 'count'),
                total_revenue=('net_amount', 'sum'),
                avg_order_value=('net_amount', 'mean'),
                total_quantity=('quantity', 'sum'),
                avg_discount=('discount', 'mean')
            )
            .round(2)
        )

    # =========================================================================
    # TOP N ANALYSIS
    # These methods demonstrate nlargest() for ranking
    # =========================================================================

    def top_products_by_quantity(self, n: int = 10) -> pd.DataFrame:
        """
        Get top N products by quantity sold.
        
        Demonstrates: groupby() + agg() + nlargest()
        
        Args:
            n: Number of top products to return (default: 10)
            
        Returns:
            pd.DataFrame: Top products with quantity, revenue, order count
        """
        return (
            self._df
            .groupby('product_name')
            .agg(
                quantity_sold=('quantity', 'sum'),
                total_revenue=('net_amount', 'sum'),
                order_count=('order_id', 'count')
            )
            .nlargest(n, 'quantity_sold')
            .round(2)
        )

    def top_products_by_revenue(self, n: int = 10) -> pd.DataFrame:
        """
        Get top N products by revenue.
        
        Args:
            n: Number of top products to return (default: 10)
            
        Returns:
            pd.DataFrame: Top products with revenue
        """
        return (
            self._df
            .groupby('product_name')['net_amount']
            .sum()
            .nlargest(n)
            .round(2)
            .to_frame('total_revenue')
        )

    def top_customers(self, n: int = 10) -> pd.DataFrame:
        """
        Get top N customers by total spending.
        
        Args:
            n: Number of top customers to return (default: 10)
            
        Returns:
            pd.DataFrame: Top customers with spending and order count
        """
        return (
            self._df
            .groupby(['customer_id', 'customer_name'])
            .agg(
                total_spent=('net_amount', 'sum'),
                order_count=('order_id', 'count'),
                avg_order_value=('net_amount', 'mean')
            )
            .nlargest(n, 'total_spent')
            .round(2)
            .reset_index()
        )

    # =========================================================================
    # FILTERING OPERATIONS
    # These methods demonstrate functional filtering with lambda predicates
    # =========================================================================

    def filter_by_category(self, category: str) -> 'SalesAnalyzer':
        """
        Filter data by product category.
        
        Returns a NEW SalesAnalyzer instance with filtered data,
        maintaining immutability of the original instance.
        
        Uses lambda expression for case-insensitive matching.
        
        Args:
            category: Category name to filter by (case-insensitive)
            
        Returns:
            SalesAnalyzer: New instance with filtered data
            
        Example:
            >>> electronics = analyzer.filter_by_category("Electronics")
            >>> electronics.total_revenue()
            8600.65
        """
        # Lambda for case-insensitive comparison
        mask = self._df['category'].apply(
            lambda x: x.lower() == category.lower()
        )
        filtered_df = self._df[mask].copy()
        return SalesAnalyzer(filtered_df)

    def filter_by_region(self, region: str) -> 'SalesAnalyzer':
        """
        Filter data by geographic region.
        
        Args:
            region: Region name to filter by (case-insensitive)
            
        Returns:
            SalesAnalyzer: New instance with filtered data
        """
        mask = self._df['region'].apply(
            lambda x: x.lower() == region.lower()
        )
        filtered_df = self._df[mask].copy()
        return SalesAnalyzer(filtered_df)

    def filter_by_date_range(self, start_date: str, end_date: str) -> 'SalesAnalyzer':
        """
        Filter data by date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format (inclusive)
            end_date: End date in YYYY-MM-DD format (inclusive)
            
        Returns:
            SalesAnalyzer: New instance with filtered data
        """
        mask = (self._df['date'] >= start_date) & (self._df['date'] <= end_date)
        filtered_df = self._df[mask].copy()
        return SalesAnalyzer(filtered_df)

    def filter_by_min_amount(self, min_amount: float) -> 'SalesAnalyzer':
        """
        Filter for orders with net amount >= threshold.
        
        Args:
            min_amount: Minimum net amount threshold
            
        Returns:
            SalesAnalyzer: New instance with filtered data
        """
        filtered_df = self._df[self._df['net_amount'] >= min_amount].copy()
        return SalesAnalyzer(filtered_df)

    def filter_by_predicate(self, predicate: Callable[[pd.Series], bool]) -> 'SalesAnalyzer':
        """
        Filter data using a custom lambda predicate.
        
        This method demonstrates functional programming by allowing
        users to pass any lambda function for filtering.
        
        Args:
            predicate: A function that takes a row (Series) and returns bool
            
        Returns:
            SalesAnalyzer: New instance with filtered data
            
        Example:
            # Filter for bulk orders (quantity > 5)
            >>> bulk = analyzer.filter_by_predicate(
            ...     lambda row: row['quantity'] > 5
            ... )
            
            # Filter for discounted orders
            >>> discounted = analyzer.filter_by_predicate(
            ...     lambda row: row['discount'] > 0
            ... )
        """
        mask = self._df.apply(predicate, axis=1)
        filtered_df = self._df[mask].copy()
        return SalesAnalyzer(filtered_df)

    # =========================================================================
    # METHOD CHAINING DEMONSTRATIONS
    # These methods show stream-like operations with pipe()
    # =========================================================================

    def analyze_high_value_orders(self, threshold: float = 100.0) -> pd.DataFrame:
        """
        Analyze high-value orders using method chaining.
        
        Demonstrates: pipe() for stream-like processing
        
        Args:
            threshold: Minimum order value to consider (default: 100.0)
            
        Returns:
            pd.DataFrame: Analysis results by category
        """
        return (
            self._df
            .pipe(lambda df: df[df['net_amount'] > threshold])  # Filter
            .groupby('category')  # Group
            .agg({
                'net_amount': ['sum', 'mean', 'count'],
                'quantity': 'sum'
            })
            .round(2)
        )

    def categorize_performance(self) -> pd.Series:
        """
        Categorize categories by performance using map() with lambda.
        
        Demonstrates: groupby() + sum() + map() with lambda
        
        Returns:
            pd.Series: Performance category (High/Medium/Low) per category
        """
        return (
            self._df
            .groupby('category')['net_amount']
            .sum()
            .map(lambda x: 'High' if x > 3000 else ('Medium' if x > 1000 else 'Low'))
        )

    # =========================================================================
    # STATISTICAL ANALYSIS
    # =========================================================================

    def descriptive_statistics(self) -> pd.DataFrame:
        """
        Get descriptive statistics for numeric columns.
        
        Uses pandas describe() for comprehensive statistics.
        
        Returns:
            pd.DataFrame: Count, mean, std, min, 25%, 50%, 75%, max
        """
        return self._df[
            ['quantity', 'unit_price', 'discount', 'gross_amount', 'net_amount']
        ].describe()

    def average_discount_by_category(self) -> pd.Series:
        """
        Calculate average discount rate by category.
        
        Returns:
            pd.Series: Category as index, average discount as values
        """
        return (
            self._df
            .groupby('category')['discount']
            .mean()
            .sort_values(ascending=False)
        )

    # =========================================================================
    # SUMMARY METHODS
    # =========================================================================

    def get_summary(self) -> Dict[str, Any]:
        """
        Get a comprehensive summary of all key metrics.
        
        Returns:
            dict: Dictionary containing all summary statistics
        """
        return {
            'total_orders': self.record_count,
            'total_revenue': round(self.total_revenue(), 2),
            'total_quantity_sold': self.total_quantity_sold(),
            'average_order_value': round(self.average_order_value(), 2),
            'total_discount_given': round(self.total_discount_given(), 2),
            'unique_customers': self.unique_customers(),
            'unique_products': self.unique_products(),
            'revenue_by_category': self.revenue_by_category().round(2).to_dict(),
            'revenue_by_region': self.revenue_by_region().round(2).to_dict(),
        }


# =============================================================================
# CONVENIENCE FUNCTION
# =============================================================================

def create_analyzer(file_path: str) -> SalesAnalyzer:
    """
    Factory function to create a SalesAnalyzer from a CSV file.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        SalesAnalyzer: Configured analyzer instance
    """
    return SalesAnalyzer(file_path)
