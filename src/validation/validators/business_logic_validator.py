"""Business logic validator."""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.validation.validators.base import Validator

if TYPE_CHECKING:
    from src.validation.report import ValidationCheck


class BusinessLogicValidator(Validator):
    """Validates business rules specific to prediction markets."""

    def run(self) -> list[ValidationCheck]:
        """Execute all business logic checks."""
        checks = []

        checks.append(self._check_market_resolution_logic())
        checks.append(self._check_price_calculation_range())
        checks.append(self._check_usdc_identification())
        checks.append(self._check_outcome_price_sum())

        return checks

    def _check_market_resolution_logic(self) -> ValidationCheck:
        """Check that resolved markets have clear winners."""
        query = f"""
        WITH market_resolution AS (
            SELECT
                id,
                closed,
                TRY_CAST(json_extract_string(outcome_prices, '$[0]') AS DOUBLE) as price_0,
                TRY_CAST(json_extract_string(outcome_prices, '$[1]') AS DOUBLE) as price_1,
                CASE
                    WHEN TRY_CAST(json_extract_string(outcome_prices, '$[0]') AS DOUBLE) > 0.99
                         AND TRY_CAST(json_extract_string(outcome_prices, '$[1]') AS DOUBLE) < 0.01 THEN 'resolved_0'
                    WHEN TRY_CAST(json_extract_string(outcome_prices, '$[0]') AS DOUBLE) < 0.01
                         AND TRY_CAST(json_extract_string(outcome_prices, '$[1]') AS DOUBLE) > 0.99 THEN 'resolved_1'
                    ELSE 'unresolved'
                END as resolution_status
            FROM '{self.data_dir}/markets/*.parquet'
            WHERE outcome_prices != '[]' AND outcome_prices IS NOT NULL
                  AND json_array_length(TRY_CAST(outcome_prices AS JSON)) = 2
        )
        SELECT
            COUNT(*) as total_markets,
            SUM(CASE WHEN closed THEN 1 ELSE 0 END) as closed_markets,
            SUM(CASE WHEN closed AND resolution_status = 'unresolved' THEN 1 ELSE 0 END) as closed_unresolved,
            SUM(CASE WHEN NOT closed AND resolution_status != 'unresolved' THEN 1 ELSE 0 END) as open_resolved
        FROM market_resolution
        """

        def validator(result):
            total, closed, closed_unresolved, open_resolved = result[0]

            if total == 0:
                return "FAIL", "No binary markets found", {"total_markets": 0}

            closed_unresolved_pct = (closed_unresolved / closed) * 100 if closed > 0 else 0

            issues = []
            if closed_unresolved_pct > 10:
                issues.append(f"{closed_unresolved_pct:.1f}% of closed markets are unresolved")
            if open_resolved > 0:
                issues.append(f"{open_resolved} open markets show resolved prices")

            if closed_unresolved_pct > 20:
                return (
                    "FAIL",
                    f"{closed_unresolved_pct:.1f}% of closed markets lack clear resolution",
                    {
                        "total_markets": total,
                        "closed_markets": closed,
                        "closed_unresolved": closed_unresolved,
                        "open_resolved": open_resolved,
                    },
                )

            if issues:
                return (
                    "WARN",
                    "; ".join(issues),
                    {
                        "total_markets": total,
                        "closed_markets": closed,
                        "closed_unresolved": closed_unresolved,
                        "open_resolved": open_resolved,
                        "closed_unresolved_pct": round(closed_unresolved_pct, 2),
                    },
                )

            return (
                "PASS",
                f"{closed - closed_unresolved:,} out of {closed:,} closed markets have clear resolution",
                {
                    "total_markets": total,
                    "closed_markets": closed,
                    "resolved_markets": closed - closed_unresolved,
                },
            )

        return self._execute_check("business_logic", "market_resolution_logic", query, validator)

    def _check_price_calculation_range(self) -> ValidationCheck:
        """Check that calculated prices fall within valid range."""
        query = f"""
        WITH ctf_prices AS (
            SELECT
                CASE
                    WHEN maker_asset_id = '0' THEN 100.0 * maker_amount / NULLIF(taker_amount, 0)
                    ELSE 100.0 * taker_amount / NULLIF(maker_amount, 0)
                END as price
            FROM '{self.data_dir}/trades/*.parquet'
            WHERE taker_amount > 0 AND maker_amount > 0
            LIMIT 100000
        )
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN price >= 1 AND price <= 99 THEN 1 ELSE 0 END) as valid_range,
            MIN(price) as min_price,
            MAX(price) as max_price
        FROM ctf_prices
        WHERE price IS NOT NULL
        """

        def validator(result):
            total, valid_range, min_price, max_price = result[0]

            if total == 0:
                return "FAIL", "No trades found for price calculation", {"total_trades": 0}

            valid_pct = (valid_range / total) * 100 if total > 0 else 0
            invalid = total - valid_range

            if valid_pct < 95:
                return (
                    "FAIL",
                    f"Only {valid_pct:.1f}% of prices are in valid range [1, 99]",
                    {
                        "total_sampled": total,
                        "valid_range_count": valid_range,
                        "invalid_count": invalid,
                        "valid_pct": round(valid_pct, 2),
                        "min_price": round(min_price, 2) if min_price else None,
                        "max_price": round(max_price, 2) if max_price else None,
                    },
                )

            if valid_pct < 99:
                return (
                    "WARN",
                    f"{invalid} prices ({100-valid_pct:.2f}%) fall outside [1, 99] range",
                    {
                        "total_sampled": total,
                        "valid_range_count": valid_range,
                        "invalid_count": invalid,
                        "valid_pct": round(valid_pct, 2),
                        "min_price": round(min_price, 2) if min_price else None,
                        "max_price": round(max_price, 2) if max_price else None,
                    },
                )

            return (
                "PASS",
                f"{valid_pct:.2f}% of sampled prices are in valid range",
                {"total_sampled": total, "valid_range_count": valid_range, "valid_pct": round(valid_pct, 2)},
            )

        return self._execute_check("business_logic", "price_calculation_range", query, validator)

    def _check_usdc_identification(self) -> ValidationCheck:
        """Check that trades have USDC on at least one side."""
        query = f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN maker_asset_id = '0' OR taker_asset_id = '0' THEN 1 ELSE 0 END) as has_usdc,
            SUM(CASE WHEN maker_asset_id = '0' AND taker_asset_id = '0' THEN 1 ELSE 0 END) as both_usdc,
            SUM(CASE WHEN maker_asset_id != '0' AND taker_asset_id != '0' THEN 1 ELSE 0 END) as no_usdc
        FROM '{self.data_dir}/trades/*.parquet'
        """

        def validator(result):
            total, has_usdc, both_usdc, no_usdc = result[0]

            if total == 0:
                return "FAIL", "No trades found", {"total_trades": 0}

            usdc_pct = (has_usdc / total) * 100 if total > 0 else 0

            if usdc_pct < 95:
                return (
                    "FAIL",
                    f"Only {usdc_pct:.1f}% of trades have USDC on at least one side",
                    {
                        "total_trades": total,
                        "trades_with_usdc": has_usdc,
                        "trades_both_usdc": both_usdc,
                        "trades_no_usdc": no_usdc,
                        "usdc_pct": round(usdc_pct, 2),
                    },
                )

            if no_usdc > 0:
                return (
                    "WARN",
                    f"{no_usdc} trades ({(no_usdc/total)*100:.3f}%) have no USDC side",
                    {
                        "total_trades": total,
                        "trades_with_usdc": has_usdc,
                        "trades_no_usdc": no_usdc,
                        "usdc_pct": round(usdc_pct, 2),
                    },
                )

            return (
                "PASS",
                f"{usdc_pct:.2f}% of trades have USDC on at least one side",
                {"total_trades": total, "trades_with_usdc": has_usdc, "usdc_pct": round(usdc_pct, 2)},
            )

        return self._execute_check("business_logic", "usdc_identification", query, validator)

    def _check_outcome_price_sum(self) -> ValidationCheck:
        """Check that active market prices sum to approximately 1.0."""
        query = f"""
        WITH active_markets AS (
            SELECT
                id,
                TRY_CAST(json_extract_string(outcome_prices, '$[0]') AS DOUBLE) as price_0,
                TRY_CAST(json_extract_string(outcome_prices, '$[1]') AS DOUBLE) as price_1
            FROM '{self.data_dir}/markets/*.parquet'
            WHERE active = true
                  AND outcome_prices != '[]'
                  AND outcome_prices IS NOT NULL
                  AND json_array_length(TRY_CAST(outcome_prices AS JSON)) = 2
        ),
        price_sums AS (
            SELECT
                id,
                price_0 + price_1 as price_sum,
                ABS((price_0 + price_1) - 1.0) as deviation
            FROM active_markets
            WHERE price_0 IS NOT NULL AND price_1 IS NOT NULL
        )
        SELECT
            COUNT(*) as total,
            AVG(deviation) as avg_deviation,
            MAX(deviation) as max_deviation,
            SUM(CASE WHEN deviation <= 0.05 THEN 1 ELSE 0 END) as within_5pct
        FROM price_sums
        """

        def validator(result):
            total, avg_dev, max_dev, within_5pct = result[0]

            if total == 0:
                return "WARN", "No active binary markets found", {"total_markets": 0}

            avg_dev_pct = avg_dev * 100 if avg_dev else 0
            max_dev_pct = max_dev * 100 if max_dev else 0
            within_5pct_rate = (within_5pct / total) * 100 if total > 0 else 0

            if avg_dev_pct > 5:
                return (
                    "WARN",
                    f"Average price sum deviation is {avg_dev_pct:.2f}% (expected <5%)",
                    {
                        "total_markets": total,
                        "avg_deviation_pct": round(avg_dev_pct, 3),
                        "max_deviation_pct": round(max_dev_pct, 3),
                        "within_5pct": within_5pct,
                        "within_5pct_rate": round(within_5pct_rate, 2),
                    },
                )

            return (
                "PASS",
                f"{within_5pct_rate:.1f}% of active markets have prices summing to ~1.0 (±5%)",
                {
                    "total_markets": total,
                    "avg_deviation_pct": round(avg_dev_pct, 3),
                    "max_deviation_pct": round(max_dev_pct, 3),
                    "within_5pct": within_5pct,
                },
            )

        return self._execute_check("business_logic", "outcome_price_sum", query, validator)
