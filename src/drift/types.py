"""Type inference and comparison for drift detection."""

import pandas as pd
from dataclasses import dataclass, field
from typing import Any


@dataclass
class TypeChange:
    """Represents a type change for a column between two dataset versions.

    Attributes:
        column: Name of the column with type change
        old_type: Type in the old dataset
        new_type: Type in the new dataset
        risk: Risk level assessment ("low", "medium", "high")
        sample_values: Examples showing the type change
    """

    column: str
    old_type: str
    new_type: str
    risk: str = "low"
    sample_values: dict[str, list[Any]] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary representation.

        Returns:
            Dictionary with all TypeChange fields
        """
        return {
            "column": self.column,
            "old_type": self.old_type,
            "new_type": self.new_type,
            "risk": self.risk,
            "sample_values": self.sample_values,
        }


class TypeChecker:
    """Infers and compares column types for drift detection.

    This class is responsible for:
    - Inferring data types from pandas Series
    - Comparing types between old and new DataFrames
    - Assessing risk levels for type changes

    Example:
        >>> checker = TypeChecker()
        >>> type_str = checker.infer_type(df["price"])
        >>> changes = checker.compare_types(old_df, new_df, common_columns)
    """

    # Supported types in order of specificity
    TYPE_PRIORITY = ["int", "float", "datetime", "bool", "string", "mixed", "empty"]

    # Risk assessment matrix: (old_type, new_type) -> risk_level
    RISK_MATRIX = {
        # Low risk: data enrichment or precision increase
        ("string", "int"): "low",
        ("string", "float"): "low",
        ("string", "bool"): "low",
        ("string", "datetime"): "low",
        ("int", "float"): "low",
        ("bool", "string"): "low",
        # Medium risk: precision loss
        ("float", "int"): "medium",
        ("datetime", "int"): "medium",
        ("datetime", "float"): "medium",
        # High risk: potential data corruption
        ("int", "string"): "high",
        ("float", "string"): "high",
        ("datetime", "string"): "high",
        ("bool", "int"): "high",
        ("int", "bool"): "high",
        ("float", "bool"): "high",
        ("bool", "float"): "high",
    }

    def __init__(self, sample_size: int = 100):
        """Initialize the TypeChecker.

        Args:
            sample_size: Number of values to sample for type inference
        """
        self.sample_size = sample_size

    def infer_type(self, series: pd.Series) -> str:
        """Infer the semantic type of a pandas Series.

        Analyzes the values in the series to determine if they represent
        integers, floats, booleans, datetime, strings, or mixed types.

        Args:
            series: The pandas Series to analyze

        Returns:
            Type string: 'int', 'float', 'bool', 'datetime', 'string', 'mixed', or 'empty'
        """
        # Handle empty series
        if len(series) == 0:
            return "empty"

        # Drop NA values for type inference
        non_null = series.dropna()

        if len(non_null) == 0:
            return "empty"

        # Check pandas dtype first for quick inference
        dtype = str(series.dtype)

        # Check for numeric types
        if "int" in dtype:
            return "int"
        elif "float" in dtype:
            # Check if it's actually integers stored as floats
            try:
                if (non_null == non_null.astype("Int64")).all():
                    return "int"
            except (ValueError, TypeError):
                pass
            return "float"
        elif "bool" in dtype:
            return "bool"
        elif "datetime" in dtype or "date" in dtype:
            return "datetime"
        elif "object" in dtype or dtype == "string":
            # For object dtype, need to infer actual type
            return self._infer_object_type(non_null)

        # Fallback for unknown types
        return "string"

    def _infer_object_type(self, series: pd.Series) -> str:
        """Infer type from an object-dtype Series.

        Args:
            series: Series with object dtype (non-null values only)

        Returns:
            Inferred type string
        """
        # Sample values for analysis
        sample = series.head(self.sample_size)

        if len(sample) == 0:
            return "empty"

        # Try to infer type from sample
        detected_types = set()

        for value in sample:
            val_type = self._detect_value_type(value)
            detected_types.add(val_type)

            # If we find mixed types early, we can stop
            if len(detected_types) > 1:
                return "mixed"

        # If all values are the same type, return that type
        if len(detected_types) == 1:
            return detected_types.pop()

        return "mixed"

    def _detect_value_type(self, value: Any) -> str:
        """Detect the type of a single value.

        Args:
            value: The value to analyze

        Returns:
            Type string for the value
        """
        if pd.isna(value):
            return "empty"

        # Convert to string for analysis
        str_val = str(value).strip()

        if str_val == "":
            return "empty"

        # Check for boolean
        if str_val.lower() in ("true", "false", "yes", "no", "1", "0"):
            # Only return bool if it's clearly a boolean context
            if str_val.lower() in ("true", "false", "yes", "no"):
                return "bool"

        # Check for datetime
        try:
            pd.to_datetime(str_val, errors="raise")
            # Make sure it's not just a number that pandas interprets as datetime
            try:
                float(str_val)
                # It's a number, not a datetime
            except ValueError:
                return "datetime"
        except (ValueError, TypeError):
            pass

        # Check for integer
        try:
            int_val = int(float(str_val))
            if int_val == float(str_val):
                return "int"
        except (ValueError, TypeError):
            pass

        # Check for float
        try:
            float(str_val)
            return "float"
        except (ValueError, TypeError):
            pass

        # Default to string
        return "string"

    def compare_types(
        self,
        old_df: pd.DataFrame,
        new_df: pd.DataFrame,
        common_columns: list[str],
    ) -> list[dict]:
        """Compare column types between two DataFrames.

        Analyzes type changes for columns that exist in both DataFrames
        and returns a list of type changes with risk assessments.

        Args:
            old_df: The original DataFrame
            new_df: The new DataFrame
            common_columns: List of columns present in both DataFrames

        Returns:
            List of dictionaries containing type change information
        """
        type_changes = []

        for column in common_columns:
            # Skip if column doesn't exist in either DataFrame
            if column not in old_df.columns or column not in new_df.columns:
                continue

            old_type = self.infer_type(old_df[column])
            new_type = self.infer_type(new_df[column])

            # Only record if there's a type change
            if old_type != new_type:
                change = TypeChange(
                    column=column,
                    old_type=old_type,
                    new_type=new_type,
                )

                # Get sample values showing the change
                change.sample_values = self._get_sample_values(
                    old_df[column], new_df[column], old_type, new_type
                )

                type_changes.append(change.to_dict())

        return type_changes

    def _get_sample_values(
        self,
        old_series: pd.Series,
        new_series: pd.Series,
        old_type: str,
        new_type: str,
        max_samples: int = 5,
    ) -> dict[str, list[Any]]:
        """Get sample values showing the type change.

        Args:
            old_series: Series from old DataFrame
            new_series: Series from new DataFrame
            old_type: Type in old DataFrame
            new_type: Type in new DataFrame
            max_samples: Maximum number of samples to include

        Returns:
            Dictionary with 'old_samples' and 'new_samples' lists
        """
        # Get non-null samples from each series
        old_samples = old_series.dropna().head(max_samples).tolist()
        new_samples = new_series.dropna().head(max_samples).tolist()

        # Convert any pandas/numpy types to native Python types for serialization
        old_samples = [self._convert_to_native(v) for v in old_samples]
        new_samples = [self._convert_to_native(v) for v in new_samples]

        return {
            "old_samples": old_samples[:max_samples],
            "new_samples": new_samples[:max_samples],
        }

    def _convert_to_native(self, value: Any) -> Any:
        """Convert pandas/numpy types to native Python types.

        Args:
            value: Value to convert

        Returns:
            Native Python type value
        """
        if pd.isna(value):
            return None

        # Handle numpy types
        if hasattr(value, "item"):
            return value.item()

        # Handle pandas Timestamp
        if isinstance(value, pd.Timestamp):
            return value.isoformat()

        return value

    def assess_risk(self, type_changes: list[dict]) -> list[dict]:
        """Assess risk level for each type change.

        Adds a 'risk' field to each type change dictionary based on
        the severity of the type transformation.

        Risk levels:
        - Low: Data enrichment, no information loss (string → numeric, int → float)
        - Medium: Minor precision loss (float → int)
        - High: Potential data corruption or significant information loss

        Args:
            type_changes: List of type change dictionaries

        Returns:
            List of type changes with risk levels added
        """
        for change in type_changes:
            old_type = change.get("old_type", "")
            new_type = change.get("new_type", "")

            # Check for mixed type (always high risk)
            if new_type == "mixed":
                change["risk"] = "high"
                continue

            if old_type == "mixed":
                change["risk"] = "medium"
                continue

            # Check for empty type transitions
            if old_type == "empty" or new_type == "empty":
                change["risk"] = "low"
                continue

            # Same type means no risk
            if old_type == new_type:
                change["risk"] = "low"
                continue

            # Look up in risk matrix
            key = (old_type, new_type)
            if key in self.RISK_MATRIX:
                change["risk"] = self.RISK_MATRIX[key]
            else:
                # Unknown transition - default to medium risk
                change["risk"] = "medium"

        return type_changes

    def get_type_summary(self, df: pd.DataFrame) -> dict[str, str]:
        """Get a summary of types for all columns in a DataFrame.

        Args:
            df: DataFrame to analyze

        Returns:
            Dictionary mapping column names to inferred types
        """
        summary = {}
        for column in df.columns:
            summary[column] = self.infer_type(df[column])
        return summary

    def get_type_counts(self, df: pd.DataFrame) -> dict[str, int]:
        """Get counts of columns by type.

        Args:
            df: DataFrame to analyze

        Returns:
            Dictionary with counts per type
        """
        type_summary = self.get_type_summary(df)
        counts: dict[str, int] = {}

        for type_str in type_summary.values():
            counts[type_str] = counts.get(type_str, 0) + 1

        return counts

    def find_type_anomalies(
        self,
        df: pd.DataFrame,
        column: str,
        expected_type: str,
    ) -> list[Any]:
        """Find values that don't match the expected type.

        Args:
            df: DataFrame to analyze
            column: Column name to check
            expected_type: Expected type for the column

        Returns:
            List of anomalous values (up to 10 samples)
        """
        series = df[column].dropna()
        anomalies = []

        for value in series.head(100):
            detected = self._detect_value_type(value)
            if detected != expected_type and detected != "empty":
                anomalies.append(self._convert_to_native(value))
                if len(anomalies) >= 10:
                    break

        return anomalies