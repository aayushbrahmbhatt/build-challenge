"""
Data Loader Service Module
==========================

This module provides the DataLoader class responsible for loading and parsing
sales data from CSV files using the pandas API. It handles file I/O, data
validation, and initial data transformation.

Design Principles:
-----------------
1. Single Responsibility: Only handles data loading and basic validation
2. Dependency Injection: File path provided at initialization
3. Error Handling: Comprehensive validation with meaningful error messages
4. Type Safety: Full type hints for all methods

Key Features:
------------
- CSV file loading with automatic type parsing
- Date parsing with configurable formats
- Data validation and integrity checks
- Support for custom column mappings

Author: [Your Name]
Date: December 2024
"""

import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Any


class DataLoader:
    """
    Service class for loading sales data from CSV files.
    
    This class encapsulates all file I/O operations and provides
    a clean interface for loading sales data into pandas DataFrames.
    It handles data type conversion, date parsing, and validation.
    
    Attributes:
        file_path (Path): Path to the CSV file
        _dataframe (pd.DataFrame): Cached DataFrame after loading
    
    Example:
        >>> loader = DataLoader("data/sales_data.csv")
        >>> df = loader.load()
        >>> print(f"Loaded {len(df)} records")
        Loaded 50 records
    """
    
    # Expected columns in the CSV file
    REQUIRED_COLUMNS = [
        'order_id', 'date', 'customer_id', 'customer_name',
        'product_id', 'product_name', 'category', 'quantity',
        'unit_price', 'discount', 'region', 'payment_method'
    ]
    
    # Data type specifications for columns
    COLUMN_DTYPES = {
        'order_id': int,
        'customer_id': str,
        'product_id': str,
        'quantity': int,
        'unit_price': float,
        'discount': float
    }

    def __init__(self, file_path: str):
        """
        Initialize the DataLoader with a file path.
        
        Args:
            file_path (str): Path to the CSV file containing sales data
            
        Raises:
            FileNotFoundError: If the specified file does not exist
            ValueError: If file_path is empty or None
        """
        # Validate input
        if not file_path:
            raise ValueError("File path cannot be empty or None")
        
        self.file_path = Path(file_path)
        
        # Verify file exists
        if not self.file_path.exists():
            raise FileNotFoundError(
                f"Data file not found: {self.file_path.absolute()}"
            )
        
        # Cache for loaded data
        self._dataframe: Optional[pd.DataFrame] = None

    def load(self) -> pd.DataFrame:
        """
        Load sales data from the CSV file.
        
        Reads the CSV file, parses dates, converts data types,
        and validates the data structure. Uses caching to avoid
        re-reading the file on subsequent calls.
        
        Returns:
            pd.DataFrame: DataFrame containing the sales data
            
        Raises:
            ValueError: If required columns are missing
            pd.errors.EmptyDataError: If the file is empty
        """
        # Return cached data if available
        if self._dataframe is not None:
            return self._dataframe.copy()
        
        # Load CSV with type specifications
        df = pd.read_csv(
            self.file_path,
            parse_dates=['date'],  # Automatic date parsing
            dtype={
                'customer_id': str,
                'product_id': str,
            }
        )
        
        # Validate required columns exist
        self._validate_columns(df)
        
        # Convert numeric columns
        df['order_id'] = df['order_id'].astype(int)
        df['quantity'] = df['quantity'].astype(int)
        df['unit_price'] = df['unit_price'].astype(float)
        df['discount'] = df['discount'].astype(float)
        
        # Cache the loaded data
        self._dataframe = df
        
        return self._dataframe.copy()

    def _validate_columns(self, df: pd.DataFrame) -> None:
        """
        Validate that all required columns are present in the DataFrame.
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            
        Raises:
            ValueError: If any required columns are missing
        """
        missing_columns = set(self.REQUIRED_COLUMNS) - set(df.columns)
        
        if missing_columns:
            raise ValueError(
                f"Missing required columns: {', '.join(sorted(missing_columns))}"
            )

    def get_info(self) -> Dict[str, Any]:
        """
        Get information about the loaded data.
        
        Returns:
            dict: Dictionary containing file path, record count,
                  column names, and data types
        """
        df = self.load()
        
        return {
            'file_path': str(self.file_path.absolute()),
            'record_count': len(df),
            'columns': list(df.columns),
            'dtypes': df.dtypes.to_dict(),
            'date_range': {
                'start': df['date'].min().strftime('%Y-%m-%d'),
                'end': df['date'].max().strftime('%Y-%m-%d')
            }
        }

    def reload(self) -> pd.DataFrame:
        """
        Force reload the data from file, clearing the cache.
        
        Returns:
            pd.DataFrame: Freshly loaded DataFrame
        """
        self._dataframe = None
        return self.load()


def load_sales_data(file_path: str) -> pd.DataFrame:
    """
    Convenience function to load sales data from a CSV file.
    
    This is a simple wrapper around the DataLoader class for
    quick one-off data loading operations.
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        pd.DataFrame: DataFrame containing the sales data
        
    Example:
        >>> df = load_sales_data("data/sales_data.csv")
        >>> print(df.head())
    """
    loader = DataLoader(file_path)
    return loader.load()
