"""
Unit Tests for Sales Analysis Application
=========================================
Consolidated test suite covering Sale model, DataLoader, and SalesAnalyzer.
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
# SALE MODEL TESTS (5 tests)
# =============================================================================

class TestSaleModel(unittest.TestCase):
    """Tests for Sale dataclass covering creation, computed properties, and immutability."""
    
    def test_sale_creation_and_properties(self):
        """Test Sale object creation with all computed properties."""
        sale = Sale(
            order_id=1001, date="2024-01-15", customer_id="C001",
            customer_name="Alice", product_id="P001", product_name="Laptop",
            category="Electronics", quantity=2, unit_price=500.00,
            discount=0.10, region="North", payment_method="Credit Card"
        )
        # Verify computed properties
        self.assertEqual(sale.gross_amount, 1000.00)  # 2 * 500
        self.assertEqual(sale.discount_amount, 100.00)  # 1000 * 0.10
        self.assertEqual(sale.net_amount, 900.00)  # 1000 - 100

    def test_sale_immutability(self):
        """Test that Sale objects are immutable (frozen dataclass)."""
        sale = Sale(
            order_id=1001, date="2024-01-15", customer_id="C001",
            customer_name="Alice", product_id="P001", product_name="Laptop",
            category="Electronics", quantity=1, unit_price=100.00,
            discount=0.0, region="North", payment_method="Cash"
        )
        with self.assertRaises(Exception):
            sale.quantity = 5

    def test_sale_to_dict(self):
        """Test Sale serialization to dictionary."""
        sale = Sale(
            order_id=1001, date="2024-01-15", customer_id="C001",
            customer_name="Alice", product_id="P001", product_name="Laptop",
            category="Electronics", quantity=1, unit_price=100.00,
            discount=0.0, region="North", payment_method="Cash"
        )
        result = sale.to_dict()
        self.assertIsInstance(result, dict)
        self.assertEqual(result['order_id'], 1001)
        self.assertIn('net_amount', result)

    def test_sale_equality(self):
        """Test equality comparison between Sale objects."""
        sale1 = Sale(1, "2024-01-01", "C1", "A", "P1", "X", "Cat", 1, 10.0, 0.0, "N", "Cash")
        sale2 = Sale(1, "2024-01-01", "C1", "A", "P1", "X", "Cat", 1, 10.0, 0.0, "N", "Cash")
        sale3 = Sale(2, "2024-01-01", "C1", "A", "P1", "X", "Cat", 1, 10.0, 0.0, "N", "Cash")
        self.assertEqual(sale1, sale2)
        self.assertNotEqual(sale1, sale3)

    def test_sale_edge_cases(self):
        """Test edge cases: zero quantity and max discount."""
        zero_qty = Sale(1, "2024-01-01", "C1", "A", "P1", "X", "Cat", 0, 100.0, 0.0, "N", "Cash")
        self.assertEqual(zero_qty.net_amount, 0.0)
        
        max_discount = Sale(1, "2024-01-01", "C1", "A", "P1", "X", "Cat", 1, 100.0, 1.0, "N", "Cash")
        self.assertEqual(max_discount.net_amount, 0.0)


# =============================================================================
# DATA LOADER TESTS (5 tests)
# =============================================================================

class TestDataLoader(unittest.TestCase):
    """Tests for DataLoader covering file loading, validation, and caching."""
    
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

    def test_load_returns_dataframe_with_correct_structure(self):
        """Test that load() returns DataFrame with correct columns and types."""
        loader = DataLoader(self.temp_file.name)
        df = loader.load()
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df['date']))
        self.assertTrue(pd.api.types.is_integer_dtype(df['order_id']))

    def test_file_not_found_error(self):
        """Test that missing file raises FileNotFoundError."""
        with self.assertRaises(FileNotFoundError):
            DataLoader("nonexistent.csv")

    def test_invalid_path_error(self):
        """Test that empty/None path raises ValueError."""
        with self.assertRaises(ValueError):
            DataLoader("")
        with self.assertRaises(ValueError):
            DataLoader(None)

    def test_get_info_returns_metadata(self):
        """Test get_info() returns proper metadata dict."""
        loader = DataLoader(self.temp_file.name)
        info = loader.get_info()
        
        self.assertIsInstance(info, dict)
        self.assertEqual(info['record_count'], 2)
        self.assertIn('date_range', info)

    def test_convenience_function(self):
        """Test load_sales_data() convenience function."""
        df = load_sales_data(self.temp_file.name)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)


# =============================================================================
# SALES ANALYZER TESTS (15 tests)
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
    """Tests for SalesAnalyzer covering aggregation, groupby, filtering, and chaining."""
    
    def setUp(self):
        self.analyzer = SalesAnalyzer(create_test_df())

    # --- Basic Aggregations ---
    def test_basic_aggregations(self):
        """Test all basic aggregation methods."""
        # Revenue: 900 + 300 + 95 + 850 + 450 = 2595
        self.assertAlmostEqual(self.analyzer.total_revenue(), 2595.00, places=2)
        self.assertEqual(self.analyzer.total_quantity_sold(), 10)
        self.assertAlmostEqual(self.analyzer.average_order_value(), 519.00, places=2)
        self.assertEqual(self.analyzer.unique_customers(), 3)
        self.assertEqual(self.analyzer.unique_products(), 4)
        self.assertAlmostEqual(self.analyzer.total_discount_given(), 305.00, places=2)
        self.assertEqual(self.analyzer.record_count, 5)

    # --- GroupBy Operations ---
    def test_revenue_by_category(self):
        """Test revenue groupby category with sorting."""
        result = self.analyzer.revenue_by_category()
        self.assertIsInstance(result, pd.Series)
        self.assertAlmostEqual(result['Electronics'], 1750.00, places=2)
        self.assertAlmostEqual(result['Furniture'], 750.00, places=2)
        # Verify sorted descending
        self.assertEqual(list(result.values), sorted(result.values, reverse=True))

    def test_revenue_by_region_and_payment(self):
        """Test revenue groupby region and payment method."""
        by_region = self.analyzer.revenue_by_region()
        self.assertAlmostEqual(by_region['North'], 1750.00, places=2)
        
        by_payment = self.analyzer.revenue_by_payment_method()
        self.assertAlmostEqual(by_payment['Credit Card'], 2200.00, places=2)

    def test_monthly_revenue(self):
        """Test monthly revenue aggregation."""
        result = self.analyzer.monthly_revenue()
        self.assertAlmostEqual(result['2024-01'], 1200.00, places=2)
        self.assertAlmostEqual(result['2024-02'], 945.00, places=2)

    def test_order_and_quantity_by_category(self):
        """Test order count and quantity by category."""
        orders = self.analyzer.order_count_by_category()
        self.assertEqual(orders['Electronics'], 2)
        
        qty = self.analyzer.quantity_sold_by_category()
        self.assertEqual(qty['Office Supplies'], 5)

    # --- Filtering Methods ---
    def test_filter_by_category(self):
        """Test category filtering returns new analyzer."""
        filtered = self.analyzer.filter_by_category('Electronics')
        self.assertIsInstance(filtered, SalesAnalyzer)
        self.assertEqual(filtered.record_count, 2)
        self.assertAlmostEqual(filtered.total_revenue(), 1750.00, places=2)

    def test_filter_by_region_date_and_amount(self):
        """Test region, date range, and min amount filtering."""
        by_region = self.analyzer.filter_by_region('North')
        self.assertEqual(by_region.record_count, 2)
        
        by_date = self.analyzer.filter_by_date_range('2024-01-01', '2024-01-31')
        self.assertEqual(by_date.record_count, 2)
        
        high_value = self.analyzer.filter_by_min_amount(400.0)
        self.assertEqual(high_value.record_count, 3)  # 900, 850, 450

    def test_filter_by_predicate_lambda(self):
        """Test filtering with lambda predicate - key functional programming feature."""
        # Quantity >= 2
        bulk = self.analyzer.filter_by_predicate(lambda row: row['quantity'] >= 2)
        self.assertEqual(bulk.record_count, 2)
        
        # Complex: Electronics with discount
        elec_disc = self.analyzer.filter_by_predicate(
            lambda row: (row['category'] == 'Electronics') & (row['discount'] > 0)
        )
        self.assertEqual(elec_disc.record_count, 2)

    def test_empty_filter_raises_error(self):
        """Test that filter returning no results raises ValueError."""
        with self.assertRaises(ValueError):
            self.analyzer.filter_by_category('NonExistent')

    # --- Method Chaining ---
    def test_method_chaining(self):
        """Test stream-like method chaining."""
        result = (self.analyzer
                  .filter_by_category('Electronics')
                  .filter_by_region('North')
                  .total_revenue())
        self.assertAlmostEqual(result, 1750.00, places=2)

    # --- Advanced Analysis ---
    def test_pivot_and_statistics(self):
        """Test pivot table and category statistics."""
        pivot = self.analyzer.category_region_pivot()
        self.assertIsInstance(pivot, pd.DataFrame)
        self.assertIn('North', pivot.columns)
        
        stats = self.analyzer.category_statistics()
        self.assertIn('total_revenue', stats.columns)

    def test_top_n_methods(self):
        """Test top N products and customers."""
        top_products = self.analyzer.top_products_by_revenue(n=2)
        self.assertIsInstance(top_products, pd.DataFrame)
        self.assertLessEqual(len(top_products), 2)
        
        top_customers = self.analyzer.top_customers(n=2)
        self.assertLessEqual(len(top_customers), 2)

    def test_get_summary(self):
        """Test summary dictionary contains all key metrics."""
        summary = self.analyzer.get_summary()
        expected_keys = ['total_revenue', 'total_quantity_sold', 'average_order_value',
                        'unique_customers', 'unique_products', 'total_discount_given']
        for key in expected_keys:
            self.assertIn(key, summary)


# =============================================================================
# EDGE CASES
# =============================================================================

class TestEdgeCases(unittest.TestCase):
    """Edge case tests."""
    
    def test_empty_dataframe_raises_error(self):
        """Test empty DataFrame raises ValueError."""
        empty_df = pd.DataFrame(columns=['order_id', 'date', 'customer_id', 'customer_name',
                                         'product_id', 'product_name', 'category', 'quantity',
                                         'unit_price', 'discount', 'region', 'payment_method'])
        with self.assertRaises(ValueError):
            SalesAnalyzer(empty_df)

    def test_single_record_dataframe(self):
        """Test analyzer works with single record."""
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
