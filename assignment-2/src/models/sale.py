"""
Sale Model Module
==================

This module defines the Sale class representing a single sales transaction record.
The class uses pandas Series internally for efficient data handling and provides
a clean interface for accessing sale properties.

Design Decisions:
-----------------
1. Uses composition over inheritance with pandas Series for data storage
2. Provides computed properties for derived values (gross_amount, net_amount)
3. Immutable design - properties are read-only
4. Type hints for better code documentation and IDE support

Author: [Your Name]
Date: December 2024
"""

from dataclasses import dataclass
from datetime import date
from typing import Any


@dataclass(frozen=True)
class Sale:
    """
    Immutable data class representing a single sales transaction.
    
    This class encapsulates all information about a sale including
    customer details, product information, pricing, and calculated
    amounts. The frozen=True makes instances immutable.
    
    Attributes:
        order_id (int): Unique identifier for the order
        date (date): Date when the transaction occurred
        customer_id (str): Unique identifier for the customer
        customer_name (str): Full name of the customer
        product_id (str): Unique identifier for the product
        product_name (str): Name/description of the product
        category (str): Product category (Electronics, Furniture, Office Supplies)
        quantity (int): Number of units purchased in this transaction
        unit_price (float): Price per unit before any discount
        discount (float): Discount rate as decimal (0.0 to 1.0, e.g., 0.10 = 10%)
        region (str): Geographic region of the sale (North, South, East, West)
        payment_method (str): Payment method used (Credit Card, Debit Card, PayPal, Cash)
    
    Example:
        >>> sale = Sale(
        ...     order_id=1001,
        ...     date=date(2024, 1, 15),
        ...     customer_id="C001",
        ...     customer_name="John Doe",
        ...     product_id="P001",
        ...     product_name="Laptop Pro 15",
        ...     category="Electronics",
        ...     quantity=2,
        ...     unit_price=1299.99,
        ...     discount=0.10,
        ...     region="North",
        ...     payment_method="Credit Card"
        ... )
        >>> sale.gross_amount
        2599.98
        >>> sale.net_amount
        2339.982
    """
    
    # Primary identifiers
    order_id: int
    date: date
    
    # Customer information
    customer_id: str
    customer_name: str
    
    # Product information
    product_id: str
    product_name: str
    category: str
    
    # Transaction details
    quantity: int
    unit_price: float
    discount: float
    
    # Classification
    region: str
    payment_method: str

    @property
    def gross_amount(self) -> float:
        """
        Calculate gross amount before discount.
        
        Formula: quantity × unit_price
        
        Returns:
            float: Total amount before discount is applied
        """
        return self.quantity * self.unit_price

    @property
    def discount_amount(self) -> float:
        """
        Calculate the discount amount.
        
        Formula: gross_amount × discount_rate
        
        Returns:
            float: The monetary value of the discount
        """
        return self.gross_amount * self.discount

    @property
    def net_amount(self) -> float:
        """
        Calculate net amount after discount.
        
        Formula: gross_amount - discount_amount
        
        Returns:
            float: Final amount after discount is applied
        """
        return self.gross_amount - self.discount_amount

    def __str__(self) -> str:
        """
        Return a human-readable string representation of the sale.
        
        Returns:
            str: Formatted string with key sale information
        """
        return (
            f"Sale(order_id={self.order_id}, "
            f"date={self.date}, "
            f"product={self.product_name}, "
            f"net_amount=${self.net_amount:.2f})"
        )

    def to_dict(self) -> dict:
        """
        Convert the Sale object to a dictionary.
        
        Useful for serialization and DataFrame conversion.
        
        Returns:
            dict: Dictionary representation of all sale attributes
        """
        return {
            'order_id': self.order_id,
            'date': self.date,
            'customer_id': self.customer_id,
            'customer_name': self.customer_name,
            'product_id': self.product_id,
            'product_name': self.product_name,
            'category': self.category,
            'quantity': self.quantity,
            'unit_price': self.unit_price,
            'discount': self.discount,
            'region': self.region,
            'payment_method': self.payment_method,
            'gross_amount': self.gross_amount,
            'discount_amount': self.discount_amount,
            'net_amount': self.net_amount
        }
