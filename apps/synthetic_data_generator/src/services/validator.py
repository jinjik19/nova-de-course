from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.services.logger import logger


class DataValidator:
    def __init__(self, df: DataFrame, total_rows: int):
        self.df = df
        self.total_rows = total_rows
        self.errors = []
        self.logger = logger

    def run(self) -> bool:
        """Start validator."""
        self.logger.info("\n--- Starting Data Validation ---")

        self._check_count()
        self._check_null_ratios(threshold=0.05, tolerance=0.015)

        if not self.errors:
            self.logger.info("✅ VALIDATION PASSED")
            return True
        else:
            self.logger.error("❌ VALIDATION FAILED", self.errors)

            for err in self.errors:
                self.logger.error(f" - {err}")

            return False

    def _check_count(self):
        """Check rows count."""
        actual_count = self.df.count()
        if actual_count != self.total_rows:
            self.errors.append(
                f"Row count mismatch: expected {self.total_rows}, got {actual_count}"
            )
        else:
            self.logger.info(f"✓ Count check passed: {actual_count} rows")

    def _check_null_ratios(self, threshold: float, tolerance: float):
        """Check null ratios for each column."""
        null_counts = self.df.select([
            F.count(F.when(F.col(c).isNull(), c)).alias(c)
            for c in self.df.columns
        ]).collect()[0].asDict()

        for col_name, null_count in null_counts.items():
            if col_name == 'id':
                if null_count > 0:
                    self.errors.append(
                        f"Column '{col_name}' has NULLs, but strictly shouldn't!"
                    )
                continue

            ratio = null_count / self.total_rows

            if self.total_rows > 100:
                if ratio > (threshold + tolerance):
                    error_msg = (
                        f"Column '{col_name}': Null ratio {ratio:.2%} "
                        f"exceeds limit {threshold:.0%} (tolerance {tolerance:.1%})"
                    )
                    self.errors.append(error_msg)
                else:
                    self.logger.info(
                        f"✓ Column '{col_name}': Null ratio {ratio:.2%} (OK)"
                    )
