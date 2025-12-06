"""
Sales Data Analysis Application - Main Entry Point
===================================================

This is the main entry point for the Sales Data Analysis application.
It demonstrates proficiency with the pandas API by performing various
aggregation and grouping operations on sales data from CSV format.

The application showcases:
1. Functional Programming: Lambda expressions, map/filter operations
2. Stream-like Operations: Method chaining with pandas API
3. Data Aggregation: GroupBy, pivot tables, multi-aggregation
4. Clean Architecture: Modular design with separate concerns

Classes Used:
------------
- SalesAnalyzer: Core analysis engine
- DataLoader: CSV file loading service  
- ReportGenerator: Output formatting utilities

Author: [Your Name]
Date: December 2024
"""

import os
import sys
from pathlib import Path

# Ensure src package is importable
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from src import SalesAnalyzer, ReportGenerator


def get_data_file_path() -> str:
    """
    Get the path to the sales data CSV file.
    
    Returns:
        str: Absolute path to sales_data.csv
    """
    return str(PROJECT_ROOT / "data" / "sales_data.csv")


def run_analysis(analyzer: SalesAnalyzer) -> None:
    """
    Execute comprehensive sales analysis and print formatted results.
    
    This function demonstrates various pandas API operations:
    - Basic aggregation (sum, mean, count)
    - GroupBy operations
    - Pivot tables
    - Filtering with lambda expressions
    - Method chaining
    
    Args:
        analyzer: Configured SalesAnalyzer instance
    """
    # =========================================================================
    # SECTION 1: Summary Statistics
    # =========================================================================
    summary = analyzer.get_summary()
    ReportGenerator.print_summary(summary)

    # =========================================================================
    # SECTION 2: Descriptive Statistics (pandas describe())
    # =========================================================================
    ReportGenerator.print_subheader("Descriptive Statistics")
    print(analyzer.descriptive_statistics().to_string())

    # =========================================================================
    # SECTION 3: GroupBy Operations
    # =========================================================================
    ReportGenerator.print_header("GROUPBY AGGREGATION OPERATIONS")
    
    # Revenue by Category - demonstrates groupby().sum()
    ReportGenerator.print_series_as_table(
        "Revenue by Category (groupby + sum)",
        analyzer.revenue_by_category()
    )
    
    # Revenue by Region
    ReportGenerator.print_series_as_table(
        "Revenue by Region",
        analyzer.revenue_by_region()
    )
    
    # Revenue by Payment Method
    ReportGenerator.print_series_as_table(
        "Revenue by Payment Method",
        analyzer.revenue_by_payment_method(),
        show_percentage=False
    )
    
    # Monthly Revenue Trend
    ReportGenerator.print_series_as_table(
        "Monthly Revenue Trend",
        analyzer.monthly_revenue(),
        show_percentage=False
    )

    # =========================================================================
    # SECTION 4: Top N Analysis (nlargest)
    # =========================================================================
    ReportGenerator.print_header("TOP N ANALYSIS")
    
    ReportGenerator.print_dataframe(
        "Top 10 Products by Quantity Sold",
        analyzer.top_products_by_quantity(10)
    )
    
    ReportGenerator.print_dataframe(
        "Top 10 Products by Revenue",
        analyzer.top_products_by_revenue(10)
    )
    
    ReportGenerator.print_dataframe(
        "Top 10 Customers by Spending",
        analyzer.top_customers(10)
    )

    # =========================================================================
    # SECTION 5: Pivot Table Analysis
    # =========================================================================
    ReportGenerator.print_header("PIVOT TABLE ANALYSIS")
    
    ReportGenerator.print_dataframe(
        "Revenue by Category × Region (pivot_table)",
        analyzer.category_region_pivot().round(2)
    )
    
    ReportGenerator.print_dataframe(
        "Category Statistics (multi-aggregation)",
        analyzer.category_statistics()
    )

    # =========================================================================
    # SECTION 6: Filtering with Lambda Expressions
    # =========================================================================
    ReportGenerator.print_header("FILTERING OPERATIONS (Lambda Expressions)")
    
    # Filter by category - demonstrates lambda in apply()
    ReportGenerator.print_subheader("Electronics Category Filter")
    electronics = analyzer.filter_by_category("Electronics")
    elec_summary = electronics.get_summary()
    print(f"  Orders: {elec_summary['total_orders']}")
    print(f"  Revenue: {ReportGenerator.format_currency(elec_summary['total_revenue'])}")
    print(f"  Avg Order Value: {ReportGenerator.format_currency(elec_summary['average_order_value'])}")
    
    # Filter by region
    ReportGenerator.print_subheader("North Region Filter")
    north = analyzer.filter_by_region("North")
    north_summary = north.get_summary()
    print(f"  Orders: {north_summary['total_orders']}")
    print(f"  Revenue: {ReportGenerator.format_currency(north_summary['total_revenue'])}")
    
    # Filter by minimum amount
    ReportGenerator.print_subheader("High-Value Orders (>=$500)")
    high_value = analyzer.filter_by_min_amount(500.0)
    hv_summary = high_value.get_summary()
    print(f"  Orders: {hv_summary['total_orders']}")
    print(f"  Revenue: {ReportGenerator.format_currency(hv_summary['total_revenue'])}")
    print(f"  Avg Order Value: {ReportGenerator.format_currency(hv_summary['average_order_value'])}")
    
    # Custom predicate filter - demonstrates lambda function
    ReportGenerator.print_subheader("Custom Lambda Filter (quantity > 2)")
    bulk_orders = analyzer.filter_by_predicate(lambda row: row['quantity'] > 2)
    bulk_summary = bulk_orders.get_summary()
    print(f"  Bulk Orders: {bulk_summary['total_orders']}")
    print(f"  Revenue: {ReportGenerator.format_currency(bulk_summary['total_revenue'])}")

    # =========================================================================
    # SECTION 7: Method Chaining (Stream-like Operations)
    # =========================================================================
    ReportGenerator.print_header("METHOD CHAINING (Stream-like Operations)")
    
    # Chained filters
    ReportGenerator.print_subheader("Chained Filters: Electronics + North Region")
    elec_north = (
        analyzer
        .filter_by_category("Electronics")
        .filter_by_region("North")
    )
    print(f"  Orders: {elec_north.record_count}")
    print(f"  Revenue: {ReportGenerator.format_currency(elec_north.total_revenue())}")
    
    # High-value order analysis with pipe()
    ReportGenerator.print_dataframe(
        "High-Value Order Analysis (pipe + groupby + agg)",
        analyzer.analyze_high_value_orders(threshold=100)
    )
    
    # Performance categorization with map() + lambda
    ReportGenerator.print_subheader("Category Performance (map with lambda)")
    performance = analyzer.categorize_performance()
    for category, level in performance.items():
        print(f"  {category}: {level}")

    # Average discount analysis
    ReportGenerator.print_subheader("Average Discount by Category")
    avg_discounts = analyzer.average_discount_by_category()
    for category, discount in avg_discounts.items():
        line = f"  {category}: {ReportGenerator.format_percentage(discount)}"
        ReportGenerator.print_line(line)

    ReportGenerator.print_footer()


def main() -> None:
    """Main entry point for the application."""
    # Print application header
    print("\n" + "=" * 70)
    print(" SALES DATA ANALYSIS APPLICATION")
    print(" Using Pandas API for Data Analysis")
    print("=" * 70)

    # Get data file path
    data_file = get_data_file_path()

    # Validate file exists
    if not os.path.exists(data_file):
        print(f"\nError: Data file not found at {data_file}")
        sys.exit(1)

    try:
        # Initialize PDF generation
        ReportGenerator.start_pdf()
        
        # Load and create analyzer
        print(f"\nLoading data from: {data_file}")
        analyzer = SalesAnalyzer(data_file)
        print(f"Loaded {analyzer.record_count} sales records.\n")
        
        # Run comprehensive analysis
        run_analysis(analyzer)
        
        # Save PDF report
        pdf_path = str(PROJECT_ROOT / "sales_report.pdf")
        ReportGenerator.save_pdf(pdf_path)
        print(f"\n*** Report exported to: {pdf_path} ***\n")
        
    except FileNotFoundError as e:
        print(f"\nError: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nError during analysis: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
