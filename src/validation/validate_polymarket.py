"""Main validation script for Polymarket data."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import duckdb

from src.validation.report import ValidationReport
from src.validation.validators import (
    BusinessLogicValidator,
    CompletenessValidator,
    ReferentialValidator,
    SchemaValidator,
    StatisticalValidator,
)


def validate_polymarket_data(data_dir: Path, output_dir: Path | None = None) -> ValidationReport:
    """Run all validation checks on Polymarket data.

    Args:
        data_dir: Path to polymarket data directory
        output_dir: Optional path to save JSON report

    Returns:
        ValidationReport with all check results
    """
    print(f"\n{'=' * 60}")
    print("Polymarket Data Validation")
    print(f"{'=' * 60}\n")
    print(f"Data Directory: {data_dir}")
    print(f"Starting validation at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    start_time = time.time()
    con = duckdb.connect()
    checks = []

    # Define validators in execution order
    validators = [
        ("Schema & Type Integrity", SchemaValidator),
        ("Referential Integrity", ReferentialValidator),
        ("Business Logic", BusinessLogicValidator),
        ("Data Completeness", CompletenessValidator),
        ("Statistical Sanity", StatisticalValidator),
    ]

    # Run each validator
    for category_name, validator_cls in validators:
        print(f"Running {category_name} checks...")
        validator = validator_cls(con, data_dir)
        category_checks = validator.run()
        checks.extend(category_checks)

        # Print quick summary
        passed = sum(1 for c in category_checks if c.status == "PASS")
        warnings = sum(1 for c in category_checks if c.status == "WARN")
        failures = sum(1 for c in category_checks if c.status == "FAIL")
        print(f"  ✓ {passed} passed, ⚠ {warnings} warnings, ✗ {failures} failures\n")

    execution_time = time.time() - start_time

    # Gather data statistics
    try:
        data_stats = _gather_data_statistics(con, data_dir)
    except Exception as e:
        print(f"Warning: Could not gather data statistics: {e}")
        data_stats = {}

    # Create report
    report = ValidationReport.from_checks(checks, data_dir, execution_time)
    report.data_statistics = data_stats

    # Calculate quality score
    quality_score = _calculate_quality_score(checks)
    report.data_statistics["quality_score"] = round(quality_score, 2)

    # Save JSON report if output directory specified
    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        json_path = output_dir / f"polymarket_validation_{timestamp}.json"
        report.save_json(json_path)
        print(f"\nJSON report saved to: {json_path}")

    return report


def _gather_data_statistics(con: duckdb.DuckDBPyConnection, data_dir: Path) -> dict:
    """Gather basic statistics about the data."""
    stats = {}

    # Markets stats
    try:
        result = con.execute(
            f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN closed THEN 1 ELSE 0 END) as closed,
                SUM(CASE WHEN clob_token_ids != '[]' THEN 1 ELSE 0 END) as ctf_markets,
                SUM(CASE WHEN market_maker_address IS NOT NULL THEN 1 ELSE 0 END) as fpmm_markets
            FROM '{data_dir}/markets/*.parquet'
            """
        ).fetchone()
        stats["markets"] = {
            "total": result[0],
            "closed": result[1],
            "ctf_markets": result[2],
            "fpmm_markets": result[3],
        }
    except Exception:
        pass

    # Trades stats
    try:
        result = con.execute(
            f"""
            SELECT
                COUNT(*) as total,
                MIN(block_number) as min_block,
                MAX(block_number) as max_block
            FROM '{data_dir}/trades/*.parquet'
            """
        ).fetchone()
        stats["ctf_trades"] = {"total": result[0], "block_range": [result[1], result[2]]}
    except Exception:
        pass

    # Legacy trades stats
    try:
        result = con.execute(
            f"""
            SELECT
                COUNT(*) as total,
                MIN(block_number) as min_block,
                MAX(block_number) as max_block
            FROM '{data_dir}/legacy_trades/*.parquet'
            """
        ).fetchone()
        if result[0] > 0:
            stats["legacy_trades"] = {"total": result[0], "block_range": [result[1], result[2]]}
    except Exception:
        pass

    return stats


def _calculate_quality_score(checks: list) -> float:
    """Calculate overall data quality score.

    Quality Score = 0.4 × referential + 0.3 × business_logic + 0.2 × completeness + 0.1 × statistical
    """
    from src.validation.report import ValidationCheck

    category_weights = {
        "referential": 0.4,
        "business_logic": 0.3,
        "completeness": 0.2,
        "statistical": 0.1,
    }

    category_scores = {}

    for category, weight in category_weights.items():
        category_checks = [c for c in checks if c.category == category]
        if not category_checks:
            continue

        # Calculate category score: PASS=1.0, WARN=0.8, FAIL=0.0
        category_score = sum(
            {"PASS": 1.0, "WARN": 0.8, "FAIL": 0.0}[c.status] for c in category_checks
        ) / len(category_checks)
        category_scores[category] = category_score

    # Weighted average
    quality_score = sum(category_scores.get(cat, 1.0) * weight for cat, weight in category_weights.items())

    return quality_score


def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description="Validate Polymarket data quality",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full validation
  python -m src.validation.validate_polymarket

  # Specify data directory
  python -m src.validation.validate_polymarket --data-dir /path/to/data/polymarket

  # Save JSON report
  python -m src.validation.validate_polymarket --output output/validation
        """,
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).parent.parent.parent / "data" / "polymarket",
        help="Path to polymarket data directory (default: data/polymarket)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output directory for JSON report (optional)",
    )

    args = parser.parse_args()

    # Validate data directory exists
    if not args.data_dir.exists():
        print(f"Error: Data directory not found: {args.data_dir}", file=sys.stderr)
        sys.exit(1)

    # Run validation
    try:
        report = validate_polymarket_data(args.data_dir, args.output)

        # Print console report
        print("\n")
        report.print_console()

        # Exit with appropriate code
        if report.status == "FAIL":
            sys.exit(1)
        elif report.status == "PASS_WITH_WARNINGS":
            sys.exit(0)
        else:
            sys.exit(0)

    except Exception as e:
        print(f"\nError during validation: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(2)


if __name__ == "__main__":
    main()
