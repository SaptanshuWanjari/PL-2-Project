"""CSV file loader and validator."""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd


@dataclass
class CSVInfo:
    file_path: Path
    columns: list[str]
    row_count: int
    column_count: int
    file_size_bytes: int
    encoding: str

    def to_dict(self) -> dict:
        return {
            "file_path": str(self.file_path),
            "columns": self.columns,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "file_size_bytes": self.file_size_bytes,
            "encoding": self.encoding,
        }


class CSVLoader:
    SUPPORTED_ENCODINGS = ["utf-8", "utf-8-sig", "latin-1", "iso-8859-1", "cp1252"]

    def __init__(self, delimiter: str = ",", strict: bool = False):
        self.delimiter = delimiter
        self.strict = strict
        self._last_encoding: Optional[str] = None

    def load(self, file_path: str | Path) -> tuple[pd.DataFrame, CSVInfo]:
        path = Path(file_path).expanduser().resolve()

        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {path}")

        if not path.is_file():
            raise ValueError(f"Path is not a file: {path}")

        file_size = path.stat().st_size
        df, encoding = self._read_with_encoding(path)

        self._validate(df, path)

        info = CSVInfo(
            file_path=path,
            columns=list(df.columns),
            row_count=len(df),
            column_count=len(df.columns),
            file_size_bytes=file_size,
            encoding=encoding,
        )

        self._last_encoding = encoding
        return df, info

    def _read_with_encoding(self, path: Path) -> tuple[pd.DataFrame, str]:
        errors = []

        for encoding in self.SUPPORTED_ENCODINGS:
            try:
                df = pd.read_csv(
                    path,
                    delimiter=self.delimiter,
                    encoding=encoding,
                    on_bad_lines="error" if self.strict else "skip",
                )
                return df, encoding
            except UnicodeDecodeError:
                errors.append(f"{encoding}: encoding error")
            except pd.errors.ParserError as e:
                if self.strict:
                    errors.append(f"{encoding}: parse error - {e}")
                else:
                    try:
                        df = pd.read_csv(
                            path,
                            delimiter=self.delimiter,
                            encoding=encoding,
                            on_bad_lines="skip",
                        )
                        return df, encoding
                    except Exception as e2:
                        errors.append(f"{encoding}: {e2}")
            except Exception as e:
                errors.append(f"{encoding}: {e}")

        error_msg = f"Could not read CSV file '{path}'. Tried encodings:\n"
        error_msg += "\n".join(errors)
        raise ValueError(error_msg)

    def _validate(self, df: pd.DataFrame, path: Path) -> None:
        if df.empty:
            raise ValueError(f"CSV file is empty: {path}")

        if len(df.columns) == 0:
            raise ValueError(f"CSV has no columns: {path}")

        # Check for duplicate columns
        duplicates = df.columns[df.columns.duplicated()].tolist()
        if duplicates:
            if self.strict:
                raise ValueError(
                    f"CSV has duplicate column names: {duplicates}. "
                    "Use --no-strict to allow this."
                )

        # Check for empty column names
        empty_cols = [col for col in df.columns if str(col).strip() == ""]
        if empty_cols:
            if self.strict:
                raise ValueError(
                    "CSV has empty column names. Use --no-strict to allow this."
                )

    def get_column_types(self, df: pd.DataFrame) -> dict[str, str]:
        types = {}
        for col in df.columns:
            types[col] = self._infer_column_type(df[col])
        return types

    def _infer_column_type(self, series: pd.Series) -> str:
        non_null = series.dropna()

        if len(non_null) == 0:
            return "empty"

        dtype = str(series.dtype)

        if "int" in dtype:
            return "int"
        elif "float" in dtype:
            if (non_null == non_null.astype(int)).all():
                return "int"
            return "float"
        elif "bool" in dtype:
            return "bool"
        elif "datetime" in dtype:
            return "datetime"
        elif "object" in dtype:
            sample = non_null.head(100)

            try:
                pd.to_datetime(sample, errors="raise")
                return "datetime"
            except (ValueError, TypeError):
                pass

            try:
                numeric = pd.to_numeric(sample, errors="raise")
                if numeric.dtype in ["int64", "int32"]:
                    return "int"
                return "float"
            except (ValueError, TypeError):
                pass

            unique_vals = set(str(v).lower() for v in sample)
            if unique_vals <= {"true", "false"} or unique_vals <= {"yes", "no"}:
                return "bool"

            return "string"

        return "unknown"

    @staticmethod
    def get_unique_values(df: pd.DataFrame, column: str, max_values: int = 100) -> list:
        unique = df[column].dropna().unique()
        return list(unique[:max_values])

    @staticmethod
    def get_null_counts(df: pd.DataFrame) -> dict[str, int]:
        return df.isnull().sum().to_dict()
