"""Validation report data structures and generation."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Literal


@dataclass
class ValidationCheck:
    """Result of a single validation check."""

    category: str  # "schema", "referential", "business_logic", "completeness", "statistical"
    name: str  # Unique identifier for this check
    status: Literal["PASS", "WARN", "FAIL"]
    message: str  # Short description
    details: dict[str, Any] = field(default_factory=dict)  # Detailed metrics
    query: str | None = None  # SQL query used (for debugging)
    execution_time_ms: float = 0.0


@dataclass
class ValidationReport:
    """Complete validation report with all checks."""

    timestamp: datetime
    data_directory: str
    status: Literal["PASS", "PASS_WITH_WARNINGS", "FAIL"]
    checks: list[ValidationCheck]
    data_statistics: dict[str, Any] = field(default_factory=dict)
    execution_time_seconds: float = 0.0

    @classmethod
    def from_checks(cls, checks: list[ValidationCheck], data_dir: str | Path, execution_time: float = 0.0) -> ValidationReport:
        """Create a validation report from a list of checks."""
        passed = sum(1 for c in checks if c.status == "PASS")
        warnings = sum(1 for c in checks if c.status == "WARN")
        failures = sum(1 for c in checks if c.status == "FAIL")

        if failures > 0:
            status = "FAIL"
        elif warnings > 0:
            status = "PASS_WITH_WARNINGS"
        else:
            status = "PASS"

        return cls(
            timestamp=datetime.now(),
            data_directory=str(data_dir),
            status=status,
            checks=checks,
            execution_time_seconds=execution_time,
        )

    @property
    def summary(self) -> dict[str, int]:
        """Get summary statistics."""
        return {
            "total_checks": len(self.checks),
            "passed": sum(1 for c in self.checks if c.status == "PASS"),
            "warnings": sum(1 for c in self.checks if c.status == "WARN"),
            "failures": sum(1 for c in self.checks if c.status == "FAIL"),
        }

    def to_json(self, indent: int = 2) -> str:
        """Convert report to JSON string."""
        data = {
            "timestamp": self.timestamp.isoformat(),
            "data_directory": self.data_directory,
            "status": self.status,
            "summary": {**self.summary, "execution_time_seconds": self.execution_time_seconds},
            "checks": [asdict(c) for c in self.checks],
            "data_statistics": self.data_statistics,
        }
        return json.dumps(data, indent=indent)

    def save_json(self, output_path: Path | str) -> None:
        """Save report to JSON file."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(self.to_json())

    def print_console(self) -> None:
        """Print report to console in human-readable format."""
        print("\n=== Polymarket Data Validation ===")
        print(f"Data Directory: {self.data_directory}")
        print(f"Started: {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        # Print each check
        for i, check in enumerate(self.checks, 1):
            status_icon = {"PASS": "✓", "WARN": "⚠", "FAIL": "✗"}[check.status]
            status_str = f"{status_icon} {check.status}"

            # Category: Name ..................... STATUS (time)
            label = f"{check.category.replace('_', ' ').title()}: {check.name.replace('_', ' ')}"
            dots = "." * max(1, 50 - len(label))
            print(f"[{i}/{len(self.checks)}] {label} {dots} {status_str} ({check.execution_time_ms / 1000:.1f}s)")

            # Show message for WARN/FAIL
            if check.status in ("WARN", "FAIL"):
                print(f"        → {check.message}")

        print()
        print("=== Summary ===")
        summary = self.summary

        if self.status == "PASS":
            print("Status: ✓ READY FOR ANALYSIS")
        elif self.status == "PASS_WITH_WARNINGS":
            print("Status: ⚠ PASSED WITH WARNINGS")
        else:
            print("Status: ✗ VALIDATION FAILED")

        print(f"Total Checks: {summary['total_checks']}")
        print(f"  ✓ Passed: {summary['passed']}")
        print(f"  ⚠ Warnings: {summary['warnings']}")
        print(f"  ✗ Failures: {summary['failures']}")
        print()
        print(f"Total Execution Time: {self.execution_time_seconds:.1f} seconds")
