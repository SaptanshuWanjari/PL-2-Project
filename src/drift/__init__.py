"""
Explainable Schema Drift Analyzer for CSV Datasets

A CLI tool that compares two versions of a CSV dataset and generates
an explainable report describing schema-level and content-level drift.
"""

__version__ = "1.0.0"
__author__ = "Saptanshu Wanjari"

from drift.loader import CSVLoader
from drift.schema import SchemaAnalyzer
from drift.types import TypeChecker
from drift.rows import RowComparator
from drift.report import ReportGenerator
from drift.explain import ExplainabilityEngine

__all__ = [
    "CSVLoader",
    "SchemaAnalyzer",
    "TypeChecker",
    "RowComparator",
    "ReportGenerator",
    "ExplainabilityEngine",
]