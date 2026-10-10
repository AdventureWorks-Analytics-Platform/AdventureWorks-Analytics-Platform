"""Deprecated compatibility exports for the Sales Performance domain."""

from src.features.Sales_Performance.domain.bronze.bronze_loader import BronzeLoader
from src.features.Sales_Performance.domain.bronze.bronze_validator import (
    BronzeValidator,
)
from src.features.Sales_Performance.domain.bronze.sales_extractor import SalesExtractor

__all__ = ["BronzeLoader", "BronzeValidator", "SalesExtractor"]
