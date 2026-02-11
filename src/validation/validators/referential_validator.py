"""Referential integrity validator."""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.validation.validators.base import Validator

if TYPE_CHECKING:
    from src.validation.report import ValidationCheck


class ReferentialValidator(Validator):
    """Validates relationships between datasets."""

    def run(self) -> list[ValidationCheck]:
        """Execute all referential integrity checks."""
        checks = []

        checks.append(self._check_ctf_trades_market_linkage())
        checks.append(self._check_legacy_trades_collateral_lookup())
        checks.append(self._check_trades_block_coverage())

        return checks

    def _check_ctf_trades_market_linkage(self) -> ValidationCheck:
        """Check that CTF trades can be linked to markets via token IDs."""
        query = f"""
        WITH trade_tokens AS (
            SELECT DISTINCT
                CASE WHEN maker_asset_id = '0' THEN taker_asset_id ELSE maker_asset_id END as token_id
            FROM '{self.data_dir}/trades/*.parquet'
            WHERE maker_asset_id = '0' OR taker_asset_id = '0'
        ),
        market_tokens AS (
            SELECT json_extract_string(clob_token_ids, '$[0]') as token_id
            FROM '{self.data_dir}/markets/*.parquet'
            WHERE clob_token_ids != '[]' AND clob_token_ids IS NOT NULL
            UNION
            SELECT json_extract_string(clob_token_ids, '$[1]')
            FROM '{self.data_dir}/markets/*.parquet'
            WHERE clob_token_ids != '[]' AND clob_token_ids IS NOT NULL
        )
        SELECT
            (SELECT COUNT(*) FROM trade_tokens) as total_trade_tokens,
            (SELECT COUNT(*) FROM market_tokens) as total_market_tokens,
            (SELECT COUNT(*) FROM trade_tokens WHERE token_id IN (SELECT token_id FROM market_tokens)) as matched_tokens
        """

        def validator(result):
            total_trade, total_market, matched = result[0]

            if total_trade == 0:
                return "FAIL", "No trade tokens found", {"total_trade_tokens": 0}

            match_rate = (matched / total_trade) * 100 if total_trade > 0 else 0
            unmatched = total_trade - matched

            if match_rate < 90:
                return (
                    "FAIL",
                    f"Only {match_rate:.1f}% of trade tokens match markets",
                    {
                        "total_trade_tokens": total_trade,
                        "total_market_tokens": total_market,
                        "matched_tokens": matched,
                        "unmatched_tokens": unmatched,
                        "match_rate": round(match_rate, 2),
                    },
                )

            if match_rate < 99:
                return (
                    "WARN",
                    f"{unmatched} trade tokens ({100-match_rate:.2f}%) cannot be matched to markets",
                    {
                        "total_trade_tokens": total_trade,
                        "total_market_tokens": total_market,
                        "matched_tokens": matched,
                        "unmatched_tokens": unmatched,
                        "match_rate": round(match_rate, 2),
                    },
                )

            return (
                "PASS",
                f"{match_rate:.2f}% of trade tokens successfully match to markets",
                {
                    "total_trade_tokens": total_trade,
                    "matched_tokens": matched,
                    "unmatched_tokens": unmatched,
                    "match_rate": round(match_rate, 2),
                },
            )

        return self._execute_check("referential", "ctf_trades_market_linkage", query, validator)

    def _check_legacy_trades_collateral_lookup(self) -> ValidationCheck:
        """Check that legacy FPMM addresses exist in collateral lookup."""
        # First check if collateral lookup exists
        collateral_lookup_path = self.data_dir / "fpmm_collateral_lookup.json"
        if not collateral_lookup_path.exists():
            from src.validation.report import ValidationCheck

            return ValidationCheck(
                category="referential",
                name="legacy_trades_collateral_lookup",
                status="WARN",
                message="FPMM collateral lookup file not found",
                details={"lookup_file_exists": False},
            )

        query = f"""
        WITH fpmm_addresses AS (
            SELECT DISTINCT LOWER(fpmm_address) as fpmm_address
            FROM '{self.data_dir}/legacy_trades/*.parquet'
        )
        SELECT
            (SELECT COUNT(*) FROM fpmm_addresses) as total_fpmm_addresses
        """

        def validator(result):
            total_fpmm = result[0][0]

            if total_fpmm == 0:
                return (
                    "WARN",
                    "No legacy trades found (expected for newer data)",
                    {"total_fpmm_addresses": 0, "lookup_file_exists": True},
                )

            # Load JSON lookup and check coverage
            import json

            with open(collateral_lookup_path) as f:
                lookup = json.load(f)

            lookup_addresses = {addr.lower() for addr in lookup.keys()}

            # Get actual FPMM addresses
            fpmm_result = self.con.execute(
                f"SELECT DISTINCT LOWER(fpmm_address) as addr FROM '{self.data_dir}/legacy_trades/*.parquet'"
            ).fetchall()
            trade_addresses = {row[0] for row in fpmm_result}

            missing = trade_addresses - lookup_addresses
            coverage_rate = ((len(trade_addresses) - len(missing)) / len(trade_addresses)) * 100 if trade_addresses else 0

            if coverage_rate < 99:
                return (
                    "WARN",
                    f"{len(missing)} FPMM addresses ({100-coverage_rate:.2f}%) not in collateral lookup",
                    {
                        "total_fpmm_addresses": len(trade_addresses),
                        "lookup_addresses": len(lookup_addresses),
                        "missing_addresses": len(missing),
                        "coverage_rate": round(coverage_rate, 2),
                    },
                )

            return (
                "PASS",
                f"All {len(trade_addresses)} FPMM addresses found in collateral lookup",
                {
                    "total_fpmm_addresses": len(trade_addresses),
                    "lookup_addresses": len(lookup_addresses),
                    "coverage_rate": 100.0,
                },
            )

        return self._execute_check("referential", "legacy_trades_collateral_lookup", query, validator)

    def _check_trades_block_coverage(self) -> ValidationCheck:
        """Check that all trades have corresponding block timestamps."""
        query = f"""
        WITH trade_block_range AS (
            SELECT
                MIN(block_number) as min_ctf_block,
                MAX(block_number) as max_ctf_block
            FROM '{self.data_dir}/trades/*.parquet'
        ),
        legacy_block_range AS (
            SELECT
                MIN(block_number) as min_legacy_block,
                MAX(block_number) as max_legacy_block
            FROM '{self.data_dir}/legacy_trades/*.parquet'
        ),
        block_coverage AS (
            SELECT
                MIN(block_number) as min_block,
                MAX(block_number) as max_block
            FROM '{self.data_dir}/blocks/*.parquet'
        )
        SELECT
            t.min_ctf_block,
            t.max_ctf_block,
            COALESCE(l.min_legacy_block, 0) as min_legacy_block,
            COALESCE(l.max_legacy_block, 0) as max_legacy_block,
            b.min_block,
            b.max_block
        FROM trade_block_range t, legacy_block_range l, block_coverage b
        """

        def validator(result):
            min_ctf, max_ctf, min_legacy, max_legacy, min_block, max_block = result[0]

            issues = []

            # Check CTF coverage
            if min_ctf < min_block:
                issues.append(f"CTF trades start at block {min_ctf}, but blocks start at {min_block}")
            if max_ctf > max_block:
                issues.append(f"CTF trades end at block {max_ctf}, but blocks end at {max_block}")

            # Check legacy coverage (if exists)
            if min_legacy > 0:
                if min_legacy < min_block:
                    issues.append(f"Legacy trades start at block {min_legacy}, but blocks start at {min_block}")
                if max_legacy > max_block:
                    issues.append(f"Legacy trades end at block {max_legacy}, but blocks end at {max_block}")

            if issues:
                return (
                    "WARN",
                    "Some trades fall outside block coverage",
                    {
                        "ctf_block_range": [min_ctf, max_ctf],
                        "legacy_block_range": [min_legacy, max_legacy] if min_legacy > 0 else None,
                        "blocks_range": [min_block, max_block],
                        "issues": issues,
                    },
                )

            return (
                "PASS",
                "All trades fall within block timestamp coverage",
                {
                    "ctf_block_range": [min_ctf, max_ctf],
                    "legacy_block_range": [min_legacy, max_legacy] if min_legacy > 0 else None,
                    "blocks_range": [min_block, max_block],
                },
            )

        return self._execute_check("referential", "trades_block_coverage", query, validator)
