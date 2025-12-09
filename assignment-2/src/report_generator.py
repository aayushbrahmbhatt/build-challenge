"""
Report Generator Module
=======================

This module provides the ReportGenerator class for creating formatted
console output and PDF reports of sales analysis results.

"""

import pandas as pd
from typing import Dict, List, Any, Optional
from fpdf import FPDF
from pathlib import Path


class PDFReport(FPDF):
    """Custom PDF class for sales reports."""
    
    def __init__(self):
        super().__init__()
        self.set_auto_page_break(auto=True, margin=15)
        
    def header(self):
        self.set_font('Helvetica', 'B', 14)
        self.cell(0, 10, 'Sales Data Analysis Report', ln=True, align='C')
        self.ln(5)
        
    def footer(self):
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', align='C')


class ReportGenerator:
    """
    Utility class for generating formatted console and PDF reports.
    """
    
    SEPARATOR_WIDTH: int = 70
    SEPARATOR_CHAR: str = "="
    
    # PDF content accumulator
    _pdf_lines: List[str] = []
    _pdf: Optional[PDFReport] = None

    @classmethod
    def start_pdf(cls) -> None:
        """Initialize PDF generation."""
        cls._pdf = PDFReport()
        cls._pdf.add_page()
        cls._pdf.set_font('Courier', '', 9)
        cls._pdf_lines = []

    @classmethod
    def _add_to_pdf(cls, text: str) -> None:
        """Add text line to PDF."""
        if cls._pdf:
            # Handle multi-line text
            for line in text.split('\n'):
                cls._pdf.cell(0, 4, line[:100], ln=True)  # Truncate long lines

    @classmethod
    def save_pdf(cls, filepath: str) -> str:
        """Save PDF to file and return the path."""
        if cls._pdf:
            cls._pdf.output(filepath)
            cls._pdf = None
            return filepath
        return ""

    @staticmethod
    def print_header(title: str) -> None:
        """Print a formatted section header."""
        line = "\n" + ReportGenerator.SEPARATOR_CHAR * ReportGenerator.SEPARATOR_WIDTH
        print(line)
        print(f" {title}")
        print(ReportGenerator.SEPARATOR_CHAR * ReportGenerator.SEPARATOR_WIDTH)
        
        ReportGenerator._add_to_pdf("")
        ReportGenerator._add_to_pdf("=" * 60)
        ReportGenerator._add_to_pdf(f" {title}")
        ReportGenerator._add_to_pdf("=" * 60)

    @staticmethod
    def print_subheader(title: str) -> None:
        """Print a formatted subsection header."""
        print(f"\n--- {title} ---")
        ReportGenerator._add_to_pdf("")
        ReportGenerator._add_to_pdf(f"--- {title} ---")

    @staticmethod
    def format_currency(amount: float) -> str:
        """Format a number as US currency."""
        return f"${amount:,.2f}"

    @staticmethod
    def format_percentage(value: float) -> str:
        """Format a decimal as percentage."""
        return f"{value * 100:.1f}%"

    @staticmethod
    def print_summary(summary: Dict[str, Any]) -> None:
        """Print a formatted summary statistics report."""
        ReportGenerator.print_header("SALES ANALYSIS SUMMARY")
        
        ReportGenerator.print_subheader("Overall Metrics")
        lines = [
            f"  Total Orders: {summary['total_orders']}",
            f"  Total Revenue: {ReportGenerator.format_currency(summary['total_revenue'])}",
            f"  Total Quantity Sold: {summary['total_quantity_sold']}",
            f"  Average Order Value: {ReportGenerator.format_currency(summary['average_order_value'])}",
            f"  Total Discounts Given: {ReportGenerator.format_currency(summary['total_discount_given'])}",
            f"  Unique Customers: {summary['unique_customers']}",
            f"  Unique Products: {summary['unique_products']}"
        ]
        for line in lines:
            print(line)
            ReportGenerator._add_to_pdf(line)

    @staticmethod
    def print_series_as_table(
        title: str,
        series: pd.Series,
        show_percentage: bool = True
    ) -> None:
        """Print a pandas Series as a formatted table."""
        ReportGenerator.print_subheader(title)
        total = series.sum()
        
        for name, value in series.items():
            formatted_value = ReportGenerator.format_currency(value)
            if show_percentage and total > 0:
                pct = (value / total) * 100
                line = f"  {name:20} {formatted_value:>15} ({pct:5.1f}%)"
            else:
                line = f"  {name:20} {formatted_value:>15}"
            print(line)
            ReportGenerator._add_to_pdf(line)

    @staticmethod
    def print_dataframe(title: str, df: pd.DataFrame) -> None:
        """Print a pandas DataFrame with a title."""
        ReportGenerator.print_subheader(title)
        df_str = df.to_string()
        print(df_str)
        for line in df_str.split('\n')[:15]:  # Limit rows in PDF
            ReportGenerator._add_to_pdf(line[:80])

    @staticmethod
    def print_line(text: str) -> None:
        """Print a single line to console and PDF."""
        print(text)
        ReportGenerator._add_to_pdf(text)

    @staticmethod
    def print_footer() -> None:
        """Print the report footer."""
        line = "\n" + ReportGenerator.SEPARATOR_CHAR * ReportGenerator.SEPARATOR_WIDTH
        print(line)
        print(" End of Report")
        print(ReportGenerator.SEPARATOR_CHAR * ReportGenerator.SEPARATOR_WIDTH + "\n")
        ReportGenerator._add_to_pdf("")
        ReportGenerator._add_to_pdf("=" * 60)
        ReportGenerator._add_to_pdf(" End of Report")
        ReportGenerator._add_to_pdf("=" * 60)
