"""Statistical sanity validator."""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.validation.validators.base import Validator

if TYPE_CHECKING:
    from src.validation.report import ValidationCheck


class StatisticalValidator(Validator):
    """Validates statistical properties and detects anomalies."""

    def run(self) -> list[ValidationCheck]:
        """Execute all statistical sanity checks."""
        checks = []

        checks.append(self._check_trade_size_outliers())
        checks.append(self._check_temporal_patterns())

        return checks

    def _check_trade_size_outliers(self) -> ValidationCheck:
        """Check for suspiciously large trades."""
        query = f"""
        WITH trade_sizes AS (
            SELECT
                CASE WHEN maker_asset_id = '0' THEN maker_amount ELSE taker_amount END / 1e6 as usdc_amount
            FROM '{self.data_dir}/trades/*.parquet'
            WHERE maker_amount > 0 AND taker_amount > 0
        )
        SELECT
            COUNT(*) as total,
            APPROX_QUANTILE(usdc_amount, 0.50) as p50_usd,
            APPROX_QUANTILE(usdc_amount, 0.90) as p90_usd,
            APPROX_QUANTILE(usdc_amount, 0.99) as p99_usd,
            APPROX_QUANTILE(usdc_amount, 0.999) as p999_usd,
            MAX(usdc_amount) as max_usd,
            SUM(CASE WHEN usdc_amount > 1000000 THEN 1 ELSE 0 END) as trades_over_1m
        FROM trade_sizes
        """

        def validator(result):
            total, p50, p90, p99, p999, max_usd, over_1m = result[0]

            if total == 0:
                return "FAIL", "No trades found for size analysis", {"total_trades": 0}

            details = {
                "total_trades": total,
                "p50_usd": round(p50, 2) if p50 else None,
                "p90_usd": round(p90, 2) if p90 else None,
                "p99_usd": round(p99, 2) if p99 else None,
                "p999_usd": round(p999, 2) if p999 else None,
                "max_trade_usd": round(max_usd, 2) if max_usd else None,
                "trades_over_1m": over_1m,
            }

            # Check for extreme outliers
            if over_1m > 0:
                over_1m_pct = (over_1m / total) * 100
                return (
                    "WARN",
                    f"{over_1m} trades exceed $1M ({over_1m_pct:.3f}%), max: ${max_usd:,.0f}",
                    details,
                )

            return (
                "PASS",
                f"Trade sizes within expected range (median: ${p50:.2f}, p99: ${p99:.0f})",
                details,
            )

        return self._execute_check("statistical", "trade_size_outliers", query, validator)

    def _check_temporal_patterns(self) -> ValidationCheck:
        """Check for suspicious temporal patterns."""
        query = f"""
        WITH blocks_with_trades AS (
            SELECT
                block_number,
                COUNT(*) as trades_in_block
            FROM '{self.data_dir}/trades/*.parquet'
            GROUP BY block_number
        )
        SELECT
            COUNT(*) as total_blocks,
            AVG(trades_in_block) as avg_trades_per_block,
            MAX(trades_in_block) as max_trades_in_block,
            APPROX_QUANTILE(trades_in_block, 0.99) as p99_trades_per_block,
            SUM(CASE WHEN trades_in_block > 1000 THEN 1 ELSE 0 END) as blocks_with_1000plus
        FROM blocks_with_trades
        """

        def validator(result):
            total_blocks, avg_trades, max_trades, p99, blocks_1000plus = result[0]

            if total_blocks == 0:
                return "FAIL", "No blocks with trades found", {"total_blocks": 0}

            details = {
                "total_blocks_with_trades": total_blocks,
                "avg_trades_per_block": round(avg_trades, 2) if avg_trades else None,
                "max_trades_in_block": max_trades,
                "p99_trades_per_block": round(p99, 2) if p99 else None,
                "blocks_with_1000plus_trades": blocks_1000plus,
            }

            # Flag blocks with extreme activity
            if blocks_1000plus > 0:
                blocks_1000plus_pct = (blocks_1000plus / total_blocks) * 100
                return (
                    "WARN",
                    f"{blocks_1000plus} blocks have >1000 trades ({blocks_1000plus_pct:.3f}%), max: {max_trades}",
                    details,
                )

            return (
                "PASS",
                f"Temporal patterns normal (avg: {avg_trades:.1f} trades/block, max: {max_trades})",
                details,
            )

        return self._execute_check("statistical", "temporal_patterns", query, validator)
