"""Schema and type integrity validator."""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.validation.validators.base import Validator

if TYPE_CHECKING:
    from src.validation.report import ValidationCheck


class SchemaValidator(Validator):
    """Validates data structure and type integrity."""

    def run(self) -> list[ValidationCheck]:
        """Execute all schema validation checks."""
        checks = []

        # Markets checks
        checks.append(self._check_markets_id_uniqueness())
        checks.append(self._check_markets_json_parsing())
        checks.append(self._check_markets_binary_structure())

        # CTF Trades checks
        checks.append(self._check_ctf_trades_required_fields())
        checks.append(self._check_ctf_trades_asset_ids())

        # Legacy Trades checks
        checks.append(self._check_legacy_trades_required_fields())
        checks.append(self._check_legacy_trades_string_integers())

        # Blocks checks
        checks.append(self._check_blocks_timestamp_format())

        return checks

    def _check_markets_id_uniqueness(self) -> ValidationCheck:
        """Check that all market IDs are unique and non-null."""
        query = f"""
        SELECT
            COUNT(*) as total_markets,
            SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) as null_ids,
            COUNT(DISTINCT id) as unique_ids
        FROM '{self.data_dir}/markets/*.parquet'
        """

        def validator(result):
            total, null_ids, unique_ids = result[0]

            if total == 0:
                return "FAIL", "No markets found in dataset", {"total_markets": 0}

            if null_ids > 0:
                return (
                    "FAIL",
                    f"{null_ids} markets have NULL IDs",
                    {"total_markets": total, "null_ids": null_ids, "unique_ids": unique_ids},
                )

            if unique_ids != total:
                duplicates = total - unique_ids
                return (
                    "FAIL",
                    f"{duplicates} duplicate market IDs found",
                    {"total_markets": total, "unique_ids": unique_ids, "duplicates": duplicates},
                )

            return (
                "PASS",
                f"All {total:,} market IDs are unique and non-null",
                {"total_markets": total, "unique_ids": unique_ids, "null_ids": 0},
            )

        return self._execute_check("schema", "markets_id_uniqueness", query, validator)

    def _check_markets_json_parsing(self) -> ValidationCheck:
        """Check that JSON fields can be parsed correctly."""
        query = f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN TRY_CAST(outcomes AS JSON) IS NULL THEN 1 ELSE 0 END) as invalid_outcomes,
            SUM(CASE WHEN TRY_CAST(outcome_prices AS JSON) IS NULL THEN 1 ELSE 0 END) as invalid_prices,
            SUM(CASE WHEN TRY_CAST(clob_token_ids AS JSON) IS NULL THEN 1 ELSE 0 END) as invalid_tokens
        FROM '{self.data_dir}/markets/*.parquet'
        """

        def validator(result):
            total, invalid_outcomes, invalid_prices, invalid_tokens = result[0]

            total_invalid = invalid_outcomes + invalid_prices + invalid_tokens
            invalid_pct = (total_invalid / (total * 3)) * 100 if total > 0 else 0

            if invalid_pct > 1.0:
                return (
                    "FAIL",
                    f"{invalid_pct:.2f}% of JSON fields cannot be parsed",
                    {
                        "total_markets": total,
                        "invalid_outcomes": invalid_outcomes,
                        "invalid_prices": invalid_prices,
                        "invalid_tokens": invalid_tokens,
                    },
                )

            if total_invalid > 0:
                return (
                    "WARN",
                    f"{total_invalid} JSON fields cannot be parsed ({invalid_pct:.3f}%)",
                    {
                        "total_markets": total,
                        "invalid_outcomes": invalid_outcomes,
                        "invalid_prices": invalid_prices,
                        "invalid_tokens": invalid_tokens,
                    },
                )

            return (
                "PASS",
                f"All JSON fields in {total:,} markets can be parsed",
                {"total_markets": total, "invalid_fields": 0},
            )

        return self._execute_check("schema", "markets_json_parsing", query, validator)

    def _check_markets_binary_structure(self) -> ValidationCheck:
        """Check that markets have binary structure (2 outcomes)."""
        query = f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE
                WHEN json_array_length(TRY_CAST(outcomes AS JSON)) = 2 THEN 1
                ELSE 0
            END) as binary_markets,
            SUM(CASE
                WHEN TRY_CAST(outcomes AS JSON) IS NULL THEN 1
                ELSE 0
            END) as null_outcomes
        FROM '{self.data_dir}/markets/*.parquet'
        WHERE outcomes != '[]' AND outcomes IS NOT NULL
        """

        def validator(result):
            total, binary_markets, null_outcomes = result[0]

            non_binary = total - binary_markets - null_outcomes
            non_binary_pct = (non_binary / total) * 100 if total > 0 else 0

            if non_binary_pct > 10:
                return (
                    "WARN",
                    f"{non_binary_pct:.1f}% of markets are not binary (expected for multi-outcome markets)",
                    {"total_markets": total, "binary_markets": binary_markets, "non_binary_markets": non_binary},
                )

            return (
                "PASS",
                f"{binary_markets:,} out of {total:,} markets have binary structure ({(binary_markets/total)*100:.1f}%)",
                {"total_markets": total, "binary_markets": binary_markets, "non_binary_markets": non_binary},
            )

        return self._execute_check("schema", "markets_binary_structure", query, validator)

    def _check_ctf_trades_required_fields(self) -> ValidationCheck:
        """Check CTF trades have required non-null fields."""
        query = f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN order_hash IS NULL THEN 1 ELSE 0 END) as null_order_hash,
            SUM(CASE WHEN block_number IS NULL OR block_number <= 0 THEN 1 ELSE 0 END) as invalid_block,
            SUM(CASE WHEN maker_amount <= 0 THEN 1 ELSE 0 END) as zero_maker_amount,
            SUM(CASE WHEN taker_amount <= 0 THEN 1 ELSE 0 END) as zero_taker_amount
        FROM '{self.data_dir}/trades/*.parquet'
        """

        def validator(result):
            total, null_hash, invalid_block, zero_maker, zero_taker = result[0]

            if total == 0:
                return "FAIL", "No CTF trades found in dataset", {"total_trades": 0}

            total_invalid = null_hash + invalid_block + zero_maker + zero_taker
            invalid_pct = (total_invalid / total) * 100 if total > 0 else 0

            if invalid_pct > 1.0:
                return (
                    "FAIL",
                    f"{invalid_pct:.2f}% of trades have invalid required fields",
                    {
                        "total_trades": total,
                        "null_order_hash": null_hash,
                        "invalid_block": invalid_block,
                        "zero_maker_amount": zero_maker,
                        "zero_taker_amount": zero_taker,
                    },
                )

            if total_invalid > 0:
                return (
                    "WARN",
                    f"{total_invalid} trades have invalid required fields ({invalid_pct:.3f}%)",
                    {
                        "total_trades": total,
                        "null_order_hash": null_hash,
                        "invalid_block": invalid_block,
                        "zero_maker_amount": zero_maker,
                        "zero_taker_amount": zero_taker,
                    },
                )

            return (
                "PASS",
                f"All required fields are valid in {total:,} CTF trades",
                {"total_trades": total, "invalid_fields": 0},
            )

        return self._execute_check("schema", "ctf_trades_required_fields", query, validator)

    def _check_ctf_trades_asset_ids(self) -> ValidationCheck:
        """Check that asset IDs can be parsed as integers (stored as strings)."""
        query = f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN TRY_CAST(maker_asset_id AS BIGINT) IS NULL THEN 1 ELSE 0 END) as invalid_maker_asset,
            SUM(CASE WHEN TRY_CAST(taker_asset_id AS BIGINT) IS NULL THEN 1 ELSE 0 END) as invalid_taker_asset
        FROM '{self.data_dir}/trades/*.parquet'
        """

        def validator(result):
            total, invalid_maker, invalid_taker = result[0]

            total_invalid = invalid_maker + invalid_taker
            invalid_pct = (total_invalid / (total * 2)) * 100 if total > 0 else 0

            if invalid_pct > 1.0:
                return (
                    "FAIL",
                    f"{invalid_pct:.2f}% of asset IDs cannot be parsed as integers",
                    {"total_trades": total, "invalid_maker_asset": invalid_maker, "invalid_taker_asset": invalid_taker},
                )

            if total_invalid > 0:
                return (
                    "WARN",
                    f"{total_invalid} asset IDs cannot be parsed ({invalid_pct:.3f}%)",
                    {"total_trades": total, "invalid_maker_asset": invalid_maker, "invalid_taker_asset": invalid_taker},
                )

            return (
                "PASS",
                f"All asset IDs in {total:,} trades can be parsed as integers",
                {"total_trades": total, "invalid_asset_ids": 0},
            )

        return self._execute_check("schema", "ctf_trades_asset_ids", query, validator)

    def _check_legacy_trades_required_fields(self) -> ValidationCheck:
        """Check legacy FPMM trades have required fields."""
        query = f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN fpmm_address IS NULL THEN 1 ELSE 0 END) as null_fpmm,
            SUM(CASE WHEN outcome_index NOT IN (0, 1) THEN 1 ELSE 0 END) as invalid_outcome_index
        FROM '{self.data_dir}/legacy_trades/*.parquet'
        """

        def validator(result):
            total, null_fpmm, invalid_index = result[0]

            if total == 0:
                return "WARN", "No legacy trades found (expected for newer data)", {"total_trades": 0}

            total_invalid = null_fpmm + invalid_index
            invalid_pct = (total_invalid / total) * 100 if total > 0 else 0

            if invalid_pct > 1.0:
                return (
                    "FAIL",
                    f"{invalid_pct:.2f}% of legacy trades have invalid fields",
                    {"total_trades": total, "null_fpmm_address": null_fpmm, "invalid_outcome_index": invalid_index},
                )

            if total_invalid > 0:
                return (
                    "WARN",
                    f"{total_invalid} legacy trades have invalid fields ({invalid_pct:.3f}%)",
                    {"total_trades": total, "null_fpmm_address": null_fpmm, "invalid_outcome_index": invalid_index},
                )

            return (
                "PASS",
                f"All required fields are valid in {total:,} legacy trades",
                {"total_trades": total, "invalid_fields": 0},
            )

        return self._execute_check("schema", "legacy_trades_required_fields", query, validator)

    def _check_legacy_trades_string_integers(self) -> ValidationCheck:
        """Check that string-encoded integers can be parsed."""
        query = f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN TRY_CAST(amount AS DOUBLE) IS NULL THEN 1 ELSE 0 END) as invalid_amount,
            SUM(CASE WHEN TRY_CAST(outcome_tokens AS DOUBLE) IS NULL THEN 1 ELSE 0 END) as invalid_tokens
        FROM '{self.data_dir}/legacy_trades/*.parquet'
        """

        def validator(result):
            total, invalid_amount, invalid_tokens = result[0]

            if total == 0:
                return "WARN", "No legacy trades found", {"total_trades": 0}

            total_invalid = invalid_amount + invalid_tokens
            invalid_pct = (total_invalid / (total * 2)) * 100 if total > 0 else 0

            if invalid_pct > 1.0:
                return (
                    "FAIL",
                    f"{invalid_pct:.2f}% of string integers cannot be parsed",
                    {"total_trades": total, "invalid_amount": invalid_amount, "invalid_tokens": invalid_tokens},
                )

            if total_invalid > 0:
                return (
                    "WARN",
                    f"{total_invalid} string integers cannot be parsed ({invalid_pct:.3f}%)",
                    {"total_trades": total, "invalid_amount": invalid_amount, "invalid_tokens": invalid_tokens},
                )

            return (
                "PASS",
                f"All string integers in {total:,} legacy trades can be parsed",
                {"total_trades": total, "invalid_strings": 0},
            )

        return self._execute_check("schema", "legacy_trades_string_integers", query, validator)

    def _check_blocks_timestamp_format(self) -> ValidationCheck:
        """Check that block timestamps are valid."""
        query = f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) as null_timestamps,
            SUM(CASE WHEN block_number IS NULL OR block_number <= 0 THEN 1 ELSE 0 END) as invalid_block_numbers
        FROM '{self.data_dir}/blocks/*.parquet'
        """

        def validator(result):
            total, null_ts, invalid_blocks = result[0]

            if total == 0:
                return "FAIL", "No block timestamp data found", {"total_blocks": 0}

            total_invalid = null_ts + invalid_blocks
            invalid_pct = (total_invalid / total) * 100 if total > 0 else 0

            if invalid_pct > 1.0:
                return (
                    "FAIL",
                    f"{invalid_pct:.2f}% of blocks have invalid timestamps or block numbers",
                    {"total_blocks": total, "null_timestamps": null_ts, "invalid_block_numbers": invalid_blocks},
                )

            if total_invalid > 0:
                return (
                    "WARN",
                    f"{total_invalid} blocks have invalid data ({invalid_pct:.3f}%)",
                    {"total_blocks": total, "null_timestamps": null_ts, "invalid_block_numbers": invalid_blocks},
                )

            return (
                "PASS",
                f"All {total:,} blocks have valid timestamps and block numbers",
                {"total_blocks": total, "invalid_entries": 0},
            )

        return self._execute_check("schema", "blocks_timestamp_format", query, validator)
