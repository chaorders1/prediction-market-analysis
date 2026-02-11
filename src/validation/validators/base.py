"""Base validator class."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

    from src.validation.report import ValidationCheck


class Validator(ABC):
    """Base class for data validators."""

    def __init__(self, con: duckdb.DuckDBPyConnection, data_dir: Path):
        self.con = con
        self.data_dir = data_dir

    @abstractmethod
    def run(self) -> list[ValidationCheck]:
        """Execute all validation checks and return results."""
        pass

    def _execute_check(
        self,
        category: str,
        name: str,
        query: str,
        validator_fn: callable,
    ) -> ValidationCheck:
        """Execute a single validation check.

        Args:
            category: Validation category (schema, referential, etc.)
            name: Check name (unique identifier)
            query: SQL query to execute
            validator_fn: Function that takes query results and returns (status, message, details)

        Returns:
            ValidationCheck with results
        """
        from src.validation.report import ValidationCheck

        start_time = time.time()

        try:
            result = self.con.execute(query).fetchall()
            status, message, details = validator_fn(result)
        except Exception as e:
            status = "FAIL"
            message = f"Query execution failed: {str(e)}"
            details = {"error": str(e)}

        execution_time_ms = (time.time() - start_time) * 1000

        return ValidationCheck(
            category=category,
            name=name,
            status=status,
            message=message,
            details=details,
            query=query,
            execution_time_ms=execution_time_ms,
        )
