"""
Models Package
==============

This package contains data model classes used throughout the Sales Analysis application.

Classes:
--------
- Sale: Represents a single sales transaction with customer, product, and pricing details

Usage:
------
    from src.models import Sale
    
    sale = Sale(
        order_id=1001,
        date=date(2024, 1, 15),
        customer_id="C001",
        customer_name="John Doe",
        ...
    )
"""

from .sale import Sale

__all__ = ['Sale']
