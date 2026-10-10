# Configuration constants for AdventureWorks Analytics Platform

"""
Application-wide configuration constants and settings.
"""

import os
from enum import Enum

# Environment
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
DEBUG = os.getenv("DEBUG", "False").lower() == "true"

# Schema names
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Default batch size for data processing
DEFAULT_BATCH_SIZE = 10000

# Supported file formats
SUPPORTED_FORMATS = ["csv", "json", "parquet", "xlsx"]

# Key source tables for Phase 0 testing
PHASE0_TEST_TABLES = [
    "Sales.Customer",
    "Sales.SalesOrderHeader",
    "Production.Product",
]


class DataLayer(Enum):
    """Data warehouse layer enumeration."""

    BRONZE = BRONZE_SCHEMA
    SILVER = SILVER_SCHEMA
    GOLD = GOLD_SCHEMA


class LoadStatus(Enum):
    """Load execution status."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    WARNING = "warning"
