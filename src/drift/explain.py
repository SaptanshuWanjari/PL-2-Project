from dataclasses import dataclass, field
from typing import Any


@dataclass
class SeverityScore:
    total: int
    level: str
    breakdown: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "level": self.level,
            "breakdown": self.breakdown,
        }


@dataclass
class ChangeExplanation:
    message: str
    category: str
    severity_contribution: int

    def to_dict(self) -> dict:
        return {
            "message": self.message,
            "category": self.category,
            "severity_contribution": self.severity_contribution,
        }


class ExplainabilityEngine:
    SEVERITY_THRESHOLDS = {
        "Low": (0, 20),
        "Medium": (21, 50),
        "High": (51, 100),
        "Critical": (101, float("inf")),
    }

    WEIGHTS = {
        "removed_column": 30,
        "type_change_string": 25,
        "type_change_other": 15,
        "rename": 10,
        "reorder": 5,
        "row_mismatch_per_100": 5,
    }

    def calculate_severity(
        self,
        schema_diff: dict,
        type_changes: list[dict],
        row_diff: dict,
    ) -> str:
        score = self._calculate_severity_score(schema_diff, type_changes, row_diff)
        return self._score_to_level(score.total)

    def calculate_severity_detailed(
        self,
        schema_diff: dict,
        type_changes: list[dict],
        row_diff: dict,
    ) -> SeverityScore:
        return self._calculate_severity_score(schema_diff, type_changes, row_diff)

    def generate_explanations(
        self,
        schema_diff: dict,
        type_changes: list[dict],
        row_diff: dict,
    ) -> list[str]:
        explanations = []

        explanations.extend(self._explain_schema_changes(schema_diff))

        explanations.extend(self._explain_type_changes(type_changes))

        explanations.extend(self._explain_row_changes(row_diff))

        if not explanations:
            explanations.append("No significant changes detected between datasets.")

        return explanations

    def explain_impact(self, severity: str, changes: dict) -> str:
        impact_templates = {
            "Low": self._explain_low_impact,
            "Medium": self._explain_medium_impact,
            "High": self._explain_high_impact,
            "Critical": self._explain_critical_impact,
        }

        explainer = impact_templates.get(severity, self._explain_low_impact)
        return explainer(changes)

    def get_change_summary(
        self,
        schema_diff: dict,
        type_changes: list[dict],
        row_diff: dict,
    ) -> dict[str, Any]:
        removed_count = len(schema_diff.get("removed", []))
        added_count = len(schema_diff.get("added", []))
        reorder_count = len(schema_diff.get("reordered", []))
        rename_count = len(schema_diff.get("renames", []))
        type_change_count = len(type_changes)

        changed_rows = len(row_diff.get("changed_rows", []))
        missing_rows = len(row_diff.get("missing_rows", []))
        new_rows = len(row_diff.get("new_rows", []))
        total_old = row_diff.get("total_old", 0)
        total_new = row_diff.get("total_new", 0)

        row_change_percentage = 0.0
        if total_old > 0:
            row_change_percentage = (changed_rows / total_old) * 100

        return {
            "schema": {
                "columns_removed": removed_count,
                "columns_added": added_count,
                "columns_reordered": reorder_count,
                "potential_renames": rename_count,
            },
            "types": {
                "type_changes": type_change_count,
                "high_risk_changes": sum(
                    1 for tc in type_changes if tc.get("risk") == "high"
                ),
                "medium_risk_changes": sum(
                    1 for tc in type_changes if tc.get("risk") == "medium"
                ),
                "low_risk_changes": sum(
                    1 for tc in type_changes if tc.get("risk") == "low"
                ),
            },
            "rows": {
                "total_old": total_old,
                "total_new": total_new,
                "missing_rows": missing_rows,
                "new_rows": new_rows,
                "changed_rows": changed_rows,
                "change_percentage": round(row_change_percentage, 2),
            },
        }

    def _calculate_severity_score(
        self,
        schema_diff: dict,
        type_changes: list[dict],
        row_diff: dict,
    ) -> SeverityScore:
        breakdown = {}

        removed_count = len(schema_diff.get("removed", []))
        if removed_count > 0:
            breakdown["removed_columns"] = (
                removed_count * self.WEIGHTS["removed_column"]
            )

        rename_count = len(schema_diff.get("renames", []))
        if rename_count > 0:
            breakdown["renames"] = rename_count * self.WEIGHTS["rename"]

        reorder_count = len(schema_diff.get("reordered", []))
        if reorder_count > 0:
            breakdown["reorders"] = reorder_count * self.WEIGHTS["reorder"]

        type_score = self._calculate_type_change_score(type_changes)
        if type_score > 0:
            breakdown["type_changes"] = type_score

        row_score = self._calculate_row_mismatch_score(row_diff)
        if row_score > 0:
            breakdown["row_mismatches"] = row_score

        total = sum(breakdown.values())
        level = self._score_to_level(total)

        return SeverityScore(total=total, level=level, breakdown=breakdown)

    def _calculate_type_change_score(self, type_changes: list[dict]) -> int:
        score = 0

        for change in type_changes:
            old_type = change.get("old_type", "")
            new_type = change.get("new_type", "")

            if old_type in ("int", "float") and new_type == "string":
                score += self.WEIGHTS["type_change_string"]
            elif old_type in ("int", "float") and new_type == "mixed":
                score += self.WEIGHTS["type_change_string"]
            else:
                score += self.WEIGHTS["type_change_other"]

        return score

    def _calculate_row_mismatch_score(self, row_diff: dict) -> int:
        changed_rows = len(row_diff.get("changed_rows", []))
        missing_rows = len(row_diff.get("missing_rows", []))
        new_rows = len(row_diff.get("new_rows", []))

        total_changes = changed_rows + missing_rows + new_rows

        score = 0
        if total_changes > 0:
            import math

            score = (
                math.ceil(total_changes / 100) * self.WEIGHTS["row_mismatch_per_100"]
            )

        return score

    def _score_to_level(self, score: int) -> str:
        if score <= 20:
            return "Low"
        elif score <= 50:
            return "Medium"
        elif score <= 100:
            return "High"
        else:
            return "Critical"

    def _explain_schema_changes(self, schema_diff: dict) -> list[str]:
        explanations = []

        removed = schema_diff.get("removed", [])
        if removed:
            columns_str = ", ".join(str(c) for c in removed[:5])
            if len(removed) > 5:
                columns_str += f", ... ({len(removed)} total)"
            explanations.append(
                f"Removed columns may break downstream systems: {columns_str}"
            )

        added = schema_diff.get("added", [])
        if added:
            columns_str = ", ".join(str(c) for c in added[:5])
            if len(added) > 5:
                columns_str += f", ... ({len(added)} total)"
            explanations.append(f"New columns added: {columns_str}")

        renames = schema_diff.get("renames", [])
        for rename in renames:
            old_name = rename.get("old_name", "unknown")
            new_name = rename.get("new_name", "unknown")
            similarity = rename.get("similarity", 0)
            explanations.append(
                f"Column renamed from '{old_name}' to '{new_name}' "
                f"(similarity: {similarity:.0%}) - verify mapping"
            )

        reordered = schema_diff.get("reordered", [])
        if reordered:
            if len(reordered) <= 3:
                for reorder in reordered:
                    col = reorder.get("column", "unknown")
                    old_idx = reorder.get("old_index", 0) + 1
                    new_idx = reorder.get("new_index", 0) + 1
                    explanations.append(
                        f"Column '{col}' moved from position {old_idx} to {new_idx}"
                    )
            else:
                explanations.append(
                    f"{len(reordered)} columns reordered - may affect column-indexed operations"
                )

        return explanations

    def _explain_type_changes(self, type_changes: list[dict]) -> list[str]:
        explanations = []

        for change in type_changes:
            column = change.get("column", "unknown")
            old_type = change.get("old_type", "unknown")
            new_type = change.get("new_type", "unknown")
            risk = change.get("risk", "medium")

            if old_type in ("int", "float") and new_type in ("string", "mixed"):
                explanations.append(
                    f"Type change from {old_type} to {new_type} in column '{column}' "
                    "suggests parsing or ingestion issue"
                )
            elif risk == "high":
                explanations.append(
                    f"High-risk type change in column '{column}': {old_type} → {new_type}"
                )
            elif risk == "medium":
                explanations.append(
                    f"Type change in column '{column}': {old_type} → {new_type}"
                )
            else:
                explanations.append(
                    f"Minor type change in column '{column}': {old_type} → {new_type}"
                )

        return explanations

    def _explain_row_changes(self, row_diff: dict) -> list[str]:
        explanations = []

        changed_rows = len(row_diff.get("changed_rows", []))
        missing_rows = len(row_diff.get("missing_rows", []))
        new_rows = len(row_diff.get("new_rows", []))
        total_old = row_diff.get("total_old", 0)
        total_new = row_diff.get("total_new", 0)

        if total_old > 0:
            change_pct = (changed_rows / total_old) * 100
            missing_pct = (missing_rows / total_old) * 100
        else:
            change_pct = 0.0
            missing_pct = 0.0

        if total_new > 0:
            new_pct = (new_rows / total_new) * 100
        else:
            new_pct = 0.0

        if changed_rows > 0:
            explanations.append(
                f"{changed_rows} rows changed, representing {change_pct:.1f}% of total"
            )

        if missing_rows > 0:
            explanations.append(
                f"{missing_rows} rows missing from new dataset ({missing_pct:.1f}% of original)"
            )

        if new_rows > 0:
            explanations.append(
                f"{new_rows} new rows added ({new_pct:.1f}% of new dataset)"
            )

        if total_old != total_new:
            diff = total_new - total_old
            direction = "increased" if diff > 0 else "decreased"
            explanations.append(
                f"Row count {direction} from {total_old} to {total_new} ({abs(diff)} rows)"
            )

        return explanations

    def _explain_low_impact(self, changes: dict) -> str:
        parts = []

        if changes.get("reorders", 0) > 0:
            parts.append("Column reordering may affect column-indexed operations.")

        if changes.get("row_changes", 0) > 0:
            parts.append(
                "Minor row changes detected. Verify data quality if unexpected."
            )

        if not parts:
            parts.append(
                "Minimal impact expected. Changes are minor and unlikely to affect "
                "downstream systems."
            )

        return " ".join(parts)

    def _explain_medium_impact(self, changes: dict) -> str:
        parts = []

        if changes.get("renames", 0) > 0:
            parts.append(
                "Column renames require updating column mappings in downstream systems."
            )

        if changes.get("type_changes", 0) > 0:
            parts.append(
                "Type changes may affect data parsing. Review affected columns for "
                "data quality issues."
            )

        if changes.get("row_changes", 0) > 0:
            parts.append(
                "Row changes detected. Consider reviewing data pipeline for consistency."
            )

        if not parts:
            parts.append(
                "Moderate impact expected. Review changes and update downstream "
                "systems as needed."
            )

        return " ".join(parts)

    def _explain_high_impact(self, changes: dict) -> str:
        """Explain high severity impact."""
        parts = []

        if changes.get("removed_columns", 0) > 0:
            parts.append(
                "REMOVED COLUMNS: Downstream systems may fail if they depend on "
                "removed columns. Immediate review required."
            )

        if changes.get("type_changes", 0) > 0:
            parts.append(
                "TYPE CHANGES: Significant type changes detected. Data quality "
                "may be compromised. Investigate ingestion pipeline."
            )

        if not parts:
            parts.append(
                "High impact detected. Multiple significant changes require review "
                "before proceeding."
            )

        return " ".join(parts)

    def _explain_critical_impact(self, changes: dict) -> str:
        parts = []

        removed = changes.get("removed_columns", 0)
        type_changes = changes.get("type_changes", 0)
        row_changes = changes.get("row_changes", 0)

        if removed > 0:
            parts.append(
                f"CRITICAL: {removed} column(s) removed. This is a breaking change "
                f"that will likely cause downstream failures."
            )

        if type_changes > 0:
            parts.append(
                f"CRITICAL: {type_changes} type change(s) detected. Data integrity "
                f"is at risk. Investigate source data immediately."
            )

        if row_changes > 0:
            parts.append(
                f"CRITICAL: Significant row changes ({row_changes}). Data pipeline "
                f"may be producing inconsistent results."
            )

        parts.append(
            "IMMEDIATE ACTION REQUIRED: Review all changes with data team before "
            "proceeding with any automated processes."
        )

        return " ".join(parts)
