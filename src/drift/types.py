from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass
class TypeChange:
    column: str
    old_type: str
    new_type: str
    risk: str = "low"
    sample_values: dict[str, list[Any]] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "column": self.column,
            "old_type": self.old_type,
            "new_type": self.new_type,
            "risk": self.risk,
            "sample_values": self.sample_values,
        }


class TypeChecker:
    TYPE_PRIORITY = ["int", "float", "datetime", "bool", "string", "mixed", "empty"]

    RISK_MATRIX = {
        ("string", "int"): "low",
        ("string", "float"): "low",
        ("string", "bool"): "low",
        ("string", "datetime"): "low",
        ("int", "float"): "low",
        ("bool", "string"): "low",
        ("float", "int"): "medium",
        ("datetime", "int"): "medium",
        ("datetime", "float"): "medium",
        ("int", "string"): "high",
        ("float", "string"): "high",
        ("datetime", "string"): "high",
        ("bool", "int"): "high",
        ("int", "bool"): "high",
        ("float", "bool"): "high",
        ("bool", "float"): "high",
    }

    def __init__(self, sample_size: int = 100):
        self.sample_size = sample_size

    def infer_type(self, series: pd.Series) -> str:
        if len(series) == 0:
            return "empty"

        non_null = series.dropna()

        if len(non_null) == 0:
            return "empty"

        dtype = str(series.dtype)

        if "int" in dtype:
            return "int"
        elif "float" in dtype:
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
            return self._infer_object_type(non_null)

        return "string"

    def _infer_object_type(self, series: pd.Series) -> str:
        sample = series.head(self.sample_size)

        if len(sample) == 0:
            return "empty"

        detected_types = set()

        for value in sample:
            val_type = self._detect_value_type(value)
            detected_types.add(val_type)

            if len(detected_types) > 1:
                return "mixed"

        if len(detected_types) == 1:
            return detected_types.pop()

        return "mixed"

    def _detect_value_type(self, value: Any) -> str:
        if pd.isna(value):
            return "empty"

        str_val = str(value).strip()

        if str_val == "":
            return "empty"

        if str_val.lower() in ("true", "false", "yes", "no", "1", "0"):
            if str_val.lower() in ("true", "false", "yes", "no"):
                return "bool"

        try:
            pd.to_datetime(str_val, errors="raise")
            try:
                float(str_val)
            except ValueError:
                return "datetime"
        except (ValueError, TypeError):
            pass

        try:
            int_val = int(float(str_val))
            if int_val == float(str_val):
                return "int"
        except (ValueError, TypeError):
            pass

        try:
            float(str_val)
            return "float"
        except (ValueError, TypeError):
            pass

        return "string"

    def compare_types(
        self,
        old_df: pd.DataFrame,
        new_df: pd.DataFrame,
        common_columns: list[str],
    ) -> list[dict]:
        type_changes = []

        for column in common_columns:
            if column not in old_df.columns or column not in new_df.columns:
                continue

            old_type = self.infer_type(old_df[column])
            new_type = self.infer_type(new_df[column])

            if old_type != new_type:
                change = TypeChange(
                    column=column,
                    old_type=old_type,
                    new_type=new_type,
                )

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
        old_samples = old_series.dropna().head(max_samples).tolist()
        new_samples = new_series.dropna().head(max_samples).tolist()

        old_samples = [self._convert_to_native(v) for v in old_samples]
        new_samples = [self._convert_to_native(v) for v in new_samples]

        return {
            "old_samples": old_samples[:max_samples],
            "new_samples": new_samples[:max_samples],
        }

    def _convert_to_native(self, value: Any) -> Any:

        if pd.isna(value):
            return None

        if hasattr(value, "item"):
            return value.item()

        if isinstance(value, pd.Timestamp):
            return value.isoformat()

        return value

    def assess_risk(self, type_changes: list[dict]) -> list[dict]:

        for change in type_changes:
            old_type = change.get("old_type", "")
            new_type = change.get("new_type", "")

            if new_type == "mixed":
                change["risk"] = "high"
                continue

            if old_type == "mixed":
                change["risk"] = "medium"
                continue

            if old_type == "empty" or new_type == "empty":
                change["risk"] = "low"
                continue

            if old_type == new_type:
                change["risk"] = "low"
                continue

            key = (old_type, new_type)
            if key in self.RISK_MATRIX:
                change["risk"] = self.RISK_MATRIX[key]
            else:
                change["risk"] = "medium"

        return type_changes

    def get_type_summary(self, df: pd.DataFrame) -> dict[str, str]:

        summary = {}
        for column in df.columns:
            summary[column] = self.infer_type(df[column])
        return summary

    def get_type_counts(self, df: pd.DataFrame) -> dict[str, int]:

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

        series = df[column].dropna()
        anomalies = []

        for value in series.head(100):
            detected = self._detect_value_type(value)
            if detected != expected_type and detected != "empty":
                anomalies.append(self._convert_to_native(value))
                if len(anomalies) >= 10:
                    break

        return anomalies
