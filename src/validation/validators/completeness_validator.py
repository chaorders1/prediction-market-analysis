"""Data completeness validator."""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.validation.validators.base import Validator

if TYPE_CHECKING:
    from src.validation.report import ValidationCheck


class CompletenessValidator(Validator):
    """Validates data completeness and coverage."""

    def run(self) -> list[ValidationCheck]:
        """Execute all completeness checks."""
        checks = []

        checks.append(self._check_duplicate_trades())
        checks.append(self._check_null_fields())

        return checks

    def _check_duplicate_trades(self) -> ValidationCheck:
        """Check for duplicate trades based on (transaction_hash, log_index)."""
        query = f"""
        WITH ctf_duplicates AS (
            SELECT
                COUNT(*) as total,
                COUNT(DISTINCT (transaction_hash, log_index)) as unique_pairs
            FROM '{self.data_dir}/trades/*.parquet'
        ),
        legacy_duplicates AS (
            SELECT
                COUNT(*) as total,
                COUNT(DISTINCT (transaction_hash, log_index)) as unique_pairs
            FROM '{self.data_dir}/legacy_trades/*.parquet'
        )
        SELECT
            c.total as ctf_total,
            c.unique_pairs as ctf_unique,
            c.total - c.unique_pairs as ctf_duplicates,
            COALESCE(l.total, 0) as legacy_total,
            COALESCE(l.unique_pairs, 0) as legacy_unique,
            COALESCE(l.total - l.unique_pairs, 0) as legacy_duplicates
        FROM ctf_duplicates c, legacy_duplicates l
        """

        def validator(result):
            ctf_total, ctf_unique, ctf_dup, legacy_total, legacy_unique, legacy_dup = result[0]

            total_dup = ctf_dup + legacy_dup
            total_records = ctf_total + legacy_total

            if total_dup > 0:
                dup_pct = (total_dup / total_records) * 100 if total_records > 0 else 0
                return (
                    "WARN" if dup_pct < 1 else "FAIL",
                    f"{total_dup} duplicate trades found ({dup_pct:.3f}%)",
                    {
                        "ctf_total": ctf_total,
                        "ctf_duplicates": ctf_dup,
                        "legacy_total": legacy_total,
                        "legacy_duplicates": legacy_dup,
                        "total_duplicates": total_dup,
                    },
                )

            return (
                "PASS",
                f"No duplicate trades found in {total_records:,} total records",
                {"ctf_total": ctf_total, "legacy_total": legacy_total, "duplicates": 0},
            )

        return self._execute_check("completeness", "duplicate_trades", query, validator)

    def _check_null_fields(self) -> ValidationCheck:
        """Check for null values in critical fields."""
        query = f"""
        WITH market_nulls AS (
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN question IS NULL OR question = '' THEN 1 ELSE 0 END) as null_question,
                SUM(CASE WHEN created_at IS NULL THEN 1 ELSE 0 END) as null_created_at
            FROM '{self.data_dir}/markets/*.parquet'
        ),
        legacy_nulls AS (
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) as null_timestamp
            FROM '{self.data_dir}/legacy_trades/*.parquet'
        )
        SELECT
            m.total as market_total,
            m.null_question,
            m.null_created_at,
            COALESCE(l.total, 0) as legacy_total,
            COALESCE(l.null_timestamp, 0) as legacy_null_timestamp
        FROM market_nulls m, legacy_nulls l
        """

        def validator(result):
            market_total, null_q, null_created, legacy_total, null_ts = result[0]

            issues = []

            # Check market question
            if null_q > 0:
                null_q_pct = (null_q / market_total) * 100 if market_total > 0 else 0
                if null_q_pct > 5:
                    issues.append(f"{null_q_pct:.1f}% of markets have null/empty questions")
                else:
                    issues.append(f"{null_q} markets have null questions ({null_q_pct:.2f}%)")

            # Check created_at
            if null_created > 0:
                null_created_pct = (null_created / market_total) * 100 if market_total > 0 else 0
                if null_created_pct > 5:
                    issues.append(f"{null_created_pct:.1f}% of markets have null created_at")

            # Legacy timestamp (acceptable to be null)
            if legacy_total > 0 and null_ts > 0:
                null_ts_pct = (null_ts / legacy_total) * 100 if legacy_total > 0 else 0
                if null_ts_pct < 100:  # Only warn if some but not all are null
                    issues.append(f"{null_ts} legacy trades ({null_ts_pct:.1f}%) have null timestamps")

            if not issues:
                return (
                    "PASS",
                    "No significant null values in critical fields",
                    {
                        "market_total": market_total,
                        "null_question": null_q,
                        "null_created_at": null_created,
                        "legacy_null_timestamp": null_ts,
                    },
                )

            # Determine severity
            has_major_issue = any(
                "%" in issue and float(issue.split("%")[0].split()[-1]) > 5 for issue in issues if "%" in issue
            )

            return (
                "WARN" if not has_major_issue else "FAIL",
                "; ".join(issues),
                {
                    "market_total": market_total,
                    "null_question": null_q,
                    "null_created_at": null_created,
                    "legacy_total": legacy_total,
                    "legacy_null_timestamp": null_ts,
                },
            )

        return self._execute_check("completeness", "null_fields", query, validator)
