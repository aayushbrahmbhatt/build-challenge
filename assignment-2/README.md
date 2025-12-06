# Build Challenge - Sales Data Analysis - Assignment 2

 A sales analysis tool that reads transaction data from a CSV file and crunches the numbers. The whole point is to demonstrate working with the pandas API - things like groupby operations, aggregations, filtering with lambdas, and method chaining.

When you run it, you get a nicely formatted report that shows:
- Basic stats (total revenue, orders, avg order value, etc.)
- Revenue breakdowns by category, region, and payment method
- Monthly trends
- Top products and customers
- And it exports everything to a PDF!

## Quick Start

# Install dependencies
pip install -r requirements.txt

# Run the analysis
python main.py

That's it. The report shows up in your terminal AND gets saved as `sales_report.pdf`.

## Project Layout

```
├── data/
│   └── sales_data.csv      # 50 sample transactions
├── src/
│   ├── models/sale.py      # Sale dataclass
│   ├── data_loader.py      # CSV loading
│   ├── sales_analyzer.py   # Main analysis logic
│   └── report_generator.py # Output formatting + PDF
├── tests/
│   └── test_all.py         # 25 tests
├── main.py                 # Entry point
└── requirements.txt
```

## The Data

CSV with these columns: order_id, date, customer_id, customer_name, product_id, product_name, category, quantity, unit_price, discount, region, payment_method.

Categories: Electronics, Furniture, Office Supplies  
Regions: North, South, East, West  
Time period: Jan-Feb 2024

## Key Features I Implemented

**Pandas stuff:**
- `groupby()` for aggregating by category/region/month
- `pivot_table()` for cross-tabulations  
- `agg()` with multiple functions
- `apply()` and `map()` with lambdas

**Functional programming:**
- Lambda expressions everywhere for filtering
- Method chaining so you can do stuff like:
  ```python
  analyzer.filter_by_category('Electronics').filter_by_region('North').total_revenue()
  ```
- Immutable patterns - filters return new objects

## Running Tests

python -m pytest tests/ -v

25 tests covering the main functionality.

## Dependencies

- pandas
- numpy  
- fpdf (for PDF generation)
- pytest (for tests)

---
Built for the Intuit Build Challenge
