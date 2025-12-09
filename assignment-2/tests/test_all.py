"""
Unit Tests for Sales Analysis Application
=========================================
Consolidated test suite covering Sale model, DataLoader, and SalesAnalyzer.
Optimized to 6 comprehensive tests with full code coverage.
"""

import unittest
import tempfile
import os
import pandas as pd
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.models.sale import Sale
from src.data_loader import DataLoader, load_sales_data
from src.sales_analyzer import SalesAnalyzer


# =============================================================================
# SALE MODEL TESTS
# =============================================================================

class TestSaleModel(unittest.TestCase):
    """Tests for Sale dataclass covering creation, computed properties, immutability, and edge cases."""
    
    def test_sale_comprehensive(self):
        """Test Sale creation, computed properties, immutability, equality, and edge cases."""
        # Test creation and computed properties
        sale = Sale(
            order_id=1001, date="2024-01-15", customer_id="C001",
            customer_name="Alice", product_id="P001", product_name="Laptop",
            category="Electronics", quantity=2, unit_price=500.00,
            discount=0.10, region="North", payment_method="Credit Card"
        )
        self.assertEqual(sale.gross_amount, 1000.00)
        self.assertEqual(sale.discount_amount, 100.00)
        self.assertEqual(sale.net_amount, 900.00)
        
        # Test immutability
        with self.assertRaises(Exception):
            sale.quantity = 5
        
        # Test to_dict
        result = sale.to_dict()
        self.assertIsInstance(result, dict)
        self.assertEqual(result['order_id'], 1001)
        self.assertIn('net_amount', result)
        
        # Test equality
        sale2 = Sale(1001, "2024-01-15", "C001", "Alice", "P001", "Laptop", 
                     "Electronics", 2, 500.00, 0.10, "North", "Credit Card")
        sale3 = Sale(1002, "2024-01-15", "C001", "Alice", "P001", "Laptop",
                     "Electronics", 2, 500.00, 0.10, "North", "Credit Card")
        self.assertEqual(sale, sale2)
        self.assertNotEqual(sale, sale3)
        
        # Test edge cases
        zero_qty = Sale(1, "2024-01-01", "C1", "A", "P1", "X", "Cat", 0, 100.0, 0.0, "N", "Cash")
        self.assertEqual(zero_qty.net_amount, 0.0)
        max_discount = Sale(1, "2024-01-01", "C1", "A", "P1", "X", "Cat", 1, 100.0, 1.0, "N", "Cash")
        self.assertEqual(max_discount.net_amount, 0.0)


# =============================================================================
# DATA LOADER TESTS
# =============================================================================

class TestDataLoader(unittest.TestCase):
    """Tests for DataLoader covering file loading, validation, error handling, and metadata."""
    
    @classmethod
    def setUpClass(cls):
        """Create temporary CSV for testing."""
        cls.temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='')
        cls.temp_file.write(
            "order_id,date,customer_id,customer_name,product_id,product_name,"
            "category,quantity,unit_price,discount,region,payment_method\n"
            "1001,2024-01-05,C001,Alice,P001,Laptop,Electronics,1,999.99,0.10,North,Credit Card\n"
            "1002,2024-01-15,C002,Bob,P002,Chair,Furniture,2,149.99,0.00,South,Cash\n"
        )
        cls.temp_file.close()

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.temp_file.name)

    def test_dataloader_comprehensive(self):
        """Test DataLoader load, validation, error handling, metadata, and convenience function."""
        # Test successful load with correct structure
        loader = DataLoader(self.temp_file.name)
        df = loader.load()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df['date']))
        self.assertTrue(pd.api.types.is_integer_dtype(df['order_id']))
        
        # Test file not found error
        with self.assertRaises(FileNotFoundError):
            DataLoader("nonexistent.csv")
        
        # Test invalid path errors
        with self.assertRaises(ValueError):
            DataLoader("")
        with self.assertRaises(ValueError):
            DataLoader(None)
        
        # Test get_info returns metadata
        info = loader.get_info()
        self.assertIsInstance(info, dict)
        self.assertEqual(info['record_count'], 2)
        self.assertIn('date_range', info)
        
        # Test convenience function
        df2 = load_sales_data(self.temp_file.name)
        self.assertIsInstance(df2, pd.DataFrame)
        self.assertEqual(len(df2), 2)


# =============================================================================
# SALES ANALYZER TESTS
# =============================================================================

def create_test_df():
    """Create test DataFrame with known values for predictable assertions."""
    return pd.DataFrame({
        'order_id': [1001, 1002, 1003, 1004, 1005],
        'date': pd.to_datetime(['2024-01-05', '2024-01-15', '2024-02-01', '2024-02-15', '2024-03-01']),
        'customer_id': ['C001', 'C002', 'C001', 'C003', 'C002'],
        'customer_name': ['Alice', 'Bob', 'Alice', 'Carol', 'Bob'],
        'product_id': ['P001', 'P002', 'P003', 'P001', 'P004'],
        'product_name': ['Laptop', 'Chair', 'Pen Set', 'Laptop', 'Desk'],
        'category': ['Electronics', 'Furniture', 'Office Supplies', 'Electronics', 'Furniture'],
        'quantity': [1, 2, 5, 1, 1],
        'unit_price': [1000.00, 150.00, 20.00, 1000.00, 500.00],
        'discount': [0.10, 0.00, 0.05, 0.15, 0.10],
        'region': ['North', 'South', 'East', 'North', 'West'],
        'payment_method': ['Credit Card', 'Cash', 'Debit Card', 'Credit Card', 'Credit Card']
    })


class TestSalesAnalyzer(unittest.TestCase):
    """Tests for SalesAnalyzer covering all aggregation, groupby, filtering, and analysis features."""
    
    def setUp(self):
        self.analyzer = SalesAnalyzer(create_test_df())

    def test_basic_aggregations_and_properties(self):
        """Test all basic aggregation methods and properties."""
        # Revenue: 900 + 300 + 95 + 850 + 450 = 2595
        self.assertAlmostEqual(self.analyzer.total_revenue(), 2595.00, places=2)
        self.assertEqual(self.analyzer.total_quantity_sold(), 10)
        self.assertAlmostEqual(self.analyzer.average_order_value(), 519.00, places=2)
        self.assertEqual(self.analyzer.unique_customers(), 3)
        self.assertEqual(self.analyzer.unique_products(), 4)
        self.assertAlmostEqual(self.analyzer.total_discount_given(), 305.00, places=2)
        self.assertEqual(self.analyzer.record_count, 5)
        
        # Test dataframe property
        self.assertIsInstance(self.analyzer.dataframe, pd.DataFrame)
        self.assertEqual(len(self.analyzer.dataframe), 5)

    def test_groupby_operations(self):
        """Test all groupby operations: category, region, payment, monthly, and statistics."""
        # Revenue by category
        by_cat = self.analyzer.revenue_by_category()
        self.assertIsInstance(by_cat, pd.Series)
        self.assertAlmostEqual(by_cat['Electronics'], 1750.00, places=2)
        self.assertAlmostEqual(by_cat['Furniture'], 750.00, places=2)
        self.assertEqual(list(by_cat.values), sorted(by_cat.values, reverse=True))
        
        # Revenue by region and payment
        by_region = self.analyzer.revenue_by_region()
        self.assertAlmostEqual(by_region['North'], 1750.00, places=2)
        by_payment = self.analyzer.revenue_by_payment_method()
        self.assertAlmostEqual(by_payment['Credit Card'], 2200.00, places=2)
        
        # Monthly revenue
        monthly = self.analyzer.monthly_revenue()
        self.assertAlmostEqual(monthly['2024-01'], 1200.00, places=2)
        self.assertAlmostEqual(monthly['2024-02'], 945.00, places=2)
        
        # Order count and quantity by category
        orders = self.analyzer.order_count_by_category()
        self.assertEqual(orders['Electronics'], 2)
        qty = self.analyzer.quantity_sold_by_category()
        self.assertEqual(qty['Office Supplies'], 5)
        
        # Pivot and statistics
        pivot = self.analyzer.category_region_pivot()
        self.assertIsInstance(pivot, pd.DataFrame)
        self.assertIn('North', pivot.columns)
        stats = self.analyzer.category_statistics()
        self.assertIn('total_revenue', stats.columns)

    def test_filtering_operations(self):
        """Test all filtering methods: category, region, date, amount, and predicate."""
        # Filter by category
        filtered = self.analyzer.filter_by_category('Electronics')
        self.assertIsInstance(filtered, SalesAnalyzer)
        self.assertEqual(filtered.record_count, 2)
        self.assertAlmostEqual(filtered.total_revenue(), 1750.00, places=2)
        
        # Filter by region
        by_region = self.analyzer.filter_by_region('North')
        self.assertEqual(by_region.record_count, 2)
        
        # Filter by date range
        by_date = self.analyzer.filter_by_date_range('2024-01-01', '2024-01-31')
        self.assertEqual(by_date.record_count, 2)
        
        # Filter by min amount
        high_value = self.analyzer.filter_by_min_amount(400.0)
        self.assertEqual(high_value.record_count, 3)
        
        # Filter by predicate (lambda)
        bulk = self.analyzer.filter_by_predicate(lambda row: row['quantity'] >= 2)
        self.assertEqual(bulk.record_count, 2)
        
        # Complex predicate
        elec_disc = self.analyzer.filter_by_predicate(
            lambda row: (row['category'] == 'Electronics') & (row['discount'] > 0)
        )
        self.assertEqual(elec_disc.record_count, 2)
        
        # Empty filter raises error
        with self.assertRaises(ValueError):
            self.analyzer.filter_by_category('NonExistent')

    def test_method_chaining_and_top_n(self):
        """Test method chaining (stream-like) and top N operations."""
        # Method chaining
        result = (self.analyzer
                  .filter_by_category('Electronics')
                  .filter_by_region('North')
                  .total_revenue())
        self.assertAlmostEqual(result, 1750.00, places=2)
        
        # Top products by revenue and quantity
        top_products = self.analyzer.top_products_by_revenue(n=2)
        self.assertIsInstance(top_products, pd.DataFrame)
        self.assertLessEqual(len(top_products), 2)
        
        top_qty = self.analyzer.top_products_by_quantity(n=2)
        self.assertIsInstance(top_qty, pd.DataFrame)
        
        # Top customers
        top_customers = self.analyzer.top_customers(n=2)
        self.assertLessEqual(len(top_customers), 2)
        
        # Summary
        summary = self.analyzer.get_summary()
        expected_keys = ['total_revenue', 'total_quantity_sold', 'average_order_value',
                        'unique_customers', 'unique_products', 'total_discount_given']
        for key in expected_keys:
            self.assertIn(key, summary)

    def test_edge_cases(self):
        """Test edge cases: empty DataFrame, single record, and initialization from file."""
        # Empty DataFrame raises error
        empty_df = pd.DataFrame(columns=['order_id', 'date', 'customer_id', 'customer_name',
                                         'product_id', 'product_name', 'category', 'quantity',
                                         'unit_price', 'discount', 'region', 'payment_method'])
        with self.assertRaises(ValueError):
            SalesAnalyzer(empty_df)
        
        # Single record works
        single_df = pd.DataFrame({
            'order_id': [1], 'date': pd.to_datetime(['2024-01-01']),
            'customer_id': ['C1'], 'customer_name': ['A'], 'product_id': ['P1'],
            'product_name': ['X'], 'category': ['Cat'], 'quantity': [1],
            'unit_price': [100.0], 'discount': [0.1], 'region': ['N'], 'payment_method': ['Cash']
        })
        analyzer = SalesAnalyzer(single_df)
        self.assertEqual(analyzer.record_count, 1)
        self.assertAlmostEqual(analyzer.total_revenue(), 90.00, places=2)


if __name__ == '__main__':
    unittest.main()
