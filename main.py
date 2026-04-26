"""
=========================================================
  Customer Transactions Data Cleaning Pipeline
=========================================================

This script implements a complete ETL (Extract, Transform, Load)
pipeline for cleaning and analyzing a raw customer transactions dataset.

It includes:
- Data loading from CSV
- Data cleaning and transformation
- Validation of business rules
- Error tracking and audit logging
- Exploratory Data Analysis (EDA)
- Data quality scoring
- Visualization dashboard generation
- Final reporting

Author: Shahab Ali
"""

# =========================================================
#   IMPORTS
# =========================================================
import pandas as pd
import numpy as np
import logging
import json
import os

import matplotlib
matplotlib.use("TkAgg")  # Fixes plotting issues in PyCharm
import matplotlib.pyplot as plt


# =========================================================
# ⚙️ CONFIGURATION
# =========================================================

# Input/output file paths
INPUT_FILE = "customer_transactions_dirty_dataset.csv"
OUTPUT_FILE = "customer_transactions_cleaned_dataset.csv"
BAD_RECORDS_FILE = "bad_records.csv"
AUDIT_LOG_FILE = "audit_log.json"

# Directory for generated plots
PLOT_DIR = "plots"
os.makedirs(PLOT_DIR, exist_ok=True)

# Required dataset columns after cleaning
REQUIRED_COLS = ["transaction_id", "customer_id", "quantity", "unit_price"]
REQUIRED_COLS = [c.lower().strip().replace(" ", "_") for c in REQUIRED_COLS]


# =========================================================
# 🪵 LOGGING CONFIGURATION
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


# =========================================================
# ERROR REPORTING CLASS
# =========================================================
class ErrorReport:
    """
    Collects all validation errors, invalid rows, and metrics
    during the ETL pipeline execution.
    """

    def __init__(self):
        self.validation_errors = []   # List of error messages
        self.invalid_rows = []        # DataFrames of bad rows
        self.metrics = {}             # Pipeline metrics

    def add_error(self, msg: str):
        """Log and store an error message."""
        self.validation_errors.append(msg)
        logging.error(msg)

    def add_invalid_rows(self, df: pd.DataFrame):
        """Store invalid rows for later analysis."""
        if df is not None and not df.empty:
            self.invalid_rows.append(df)

    def finalize(self):
        """Combine all invalid rows into a single DataFrame."""
        if self.invalid_rows:
            self.invalid_rows = pd.concat(self.invalid_rows, ignore_index=True).drop_duplicates()
        else:
            self.invalid_rows = pd.DataFrame()

    def to_dict(self):
        """Convert error report to dictionary format for JSON export."""
        return {
            "validation_errors": self.validation_errors,
            "invalid_rows_count": len(self.invalid_rows),
            "metrics": self.metrics
        }

    def save_audit_log(self):
        """Save audit log as JSON file."""
        with open(AUDIT_LOG_FILE, "w") as f:
            json.dump(self.to_dict(), f, indent=4)
        logging.info(f"Audit log saved → {AUDIT_LOG_FILE}")


# =========================================================
#   DATA LOADING CLASS
# =========================================================
class CSVDataSource:
    """
    Responsible for loading CSV dataset into a Pandas DataFrame.
    """

    def __init__(self, path: str):
        self.path = path

    def load(self) -> pd.DataFrame:
        """Load dataset and standardize column names."""
        logging.info(f"Loading dataset from {self.path}")
        df = pd.read_csv(self.path)

        # Normalize column names
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

        logging.info(f"Loaded dataset shape: {df.shape}")
        return df


# =========================================================
#   DATA TRANSFORMATION CLASSES
# =========================================================

class StandardizeValues:
    """Standardizes text columns (trim, lowercase, normalize values)."""

    def transform(self, df):
        df = df.copy()

        obj_cols = df.select_dtypes(include="object").columns

        for col in obj_cols:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.lower()
                .replace("nan", np.nan)
            )

        # Normalize gender values if present
        if "gender" in df.columns:
            df["gender"] = df["gender"].replace({
                "m": "male",
                "f": "female"
            })

        return df


class CleanNumeric:
    """Converts numeric columns safely."""

    def transform(self, df):
        df = df.copy()

        for col in ["quantity", "unit_price", "transaction_amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df


class FixTransactionAmount:
    """Recalculates transaction amount for consistency."""

    def transform(self, df):
        df = df.copy()

        if {"quantity", "unit_price"}.issubset(df.columns):
            df["transaction_amount"] = (
                df["quantity"] * df["unit_price"]
            ).round(2)

        return df


class RemoveDuplicates:
    """Removes duplicate rows from dataset."""

    def transform(self, df):
        return df.drop_duplicates()


class DropInvalidRows:
    """
    Removes rows that violate required rules:
    - Missing required columns
    - Null values in key fields
    - Invalid numeric values (<= 0)
    """

    def transform(self, df, error_report: ErrorReport):
        df = df.copy()

        # Check missing columns
        missing_cols = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing_cols:
            error_report.add_error(f"Missing columns: {missing_cols}")
            return df

        invalid_mask = pd.Series(False, index=df.index)

        # Missing required fields
        invalid_mask |= df[REQUIRED_COLS].isna().any(axis=1)

        # Invalid numeric values
        if {"quantity", "unit_price"}.issubset(df.columns):
            invalid_mask |= (df["quantity"] <= 0) | (df["unit_price"] <= 0)

        # Store invalid rows
        bad_rows = df[invalid_mask]
        error_report.add_invalid_rows(bad_rows)

        # Remove invalid rows
        df = df[~invalid_mask]

        logging.info(f"Invalid rows removed: {invalid_mask.sum()}")
        return df


# =========================================================
#   DATA VALIDATION
# =========================================================
class TransactionValidator:
    """
    Ensures calculated transaction values are consistent.
    """

    def validate(self, df, error_report: ErrorReport):

        required = {"quantity", "unit_price", "transaction_amount"}

        if not required.issubset(df.columns):
            error_report.add_error("Missing required transaction columns")
            return

        expected = (df["quantity"] * df["unit_price"]).round(2)

        valid_mask = (
            df["transaction_amount"].notna() &
            df["quantity"].notna() &
            df["unit_price"].notna()
        )

        mismatches = valid_mask & ~np.isclose(
            df["transaction_amount"],
            expected,
            atol=0.01
        )

        bad = df[mismatches]

        if not bad.empty:
            error_report.add_error(f"Transaction mismatches: {len(bad)}")
            error_report.add_invalid_rows(bad)

        logging.info("Validation completed")


# =========================================================
#   EXPLORATORY DATA ANALYSIS (EDA)
# =========================================================
class EDA:
    """Performs summary statistics and insights generation."""

    def run(self, df):
        logging.info("========== EDA START ==========")
        logging.info(f"Shape: {df.shape}")

        # Missing values
        missing = df.isnull().sum()
        logging.info(f"\nMissing values:\n{missing[missing > 0]}")

        # Summary statistics
        logging.info("\nDESCRIPTIVE STATS:")
        logging.info(df.describe(include="all").to_string())

        # Top values for categorical columns
        for col in df.select_dtypes(include="object").columns[:5]:
            logging.info(f"\nTOP VALUES - {col}")
            logging.info(df[col].value_counts().head(10).to_string())

        # Top customers by spending
        if "customer_id" in df.columns and "transaction_amount" in df.columns:
            logging.info("\nTOP CUSTOMERS BY SPEND:")
            logging.info(
                df.groupby("customer_id")["transaction_amount"]
                .sum()
                .sort_values(ascending=False)
                .head(10)
                .to_string()
            )

        logging.info("========== EDA END ==========")


# =========================================================
#   DATA QUALITY SCORER
# =========================================================
class DataQualityScorer:
    """Computes overall dataset quality score."""

    def score(self, df, error_report, start_rows):

        completeness = 1 - (df.isna().sum().sum() / (df.shape[0] * df.shape[1]))
        validity = len(df) / start_rows
        duplicate_penalty = 1 - (error_report.metrics.get("duplicates_removed", 0) / start_rows)

        score = (
            (completeness * 0.4) +
            (validity * 0.4) +
            (duplicate_penalty * 0.2)
        ) * 100

        return round(score, 2)


# =========================================================
#   VISUALIZATION DASHBOARD
# =========================================================
class DataQualityDashboard:
    """Generates plots for data quality analysis."""

    def plot(self, df, start_rows, final_rows, score):

        missing = df.isnull().sum()

        # Missing values plot
        plt.figure()
        missing[missing > 0].plot(kind="bar")
        plt.title("Missing Values Per Column")
        plt.tight_layout()
        plt.savefig(f"{PLOT_DIR}/missing_values.png")
        plt.close()

        # Before vs After cleaning
        plt.figure()
        plt.bar(["Start", "Final"], [start_rows, final_rows])
        plt.title("Before vs After Cleaning")
        plt.tight_layout()
        plt.savefig(f"{PLOT_DIR}/before_after.png")
        plt.close()

        # Data quality score
        plt.figure()
        plt.bar(["Data Quality Score"], [score])
        plt.ylim(0, 100)
        plt.title("Data Quality Score")
        plt.tight_layout()
        plt.savefig(f"{PLOT_DIR}/data_quality_score.png")
        plt.close()

        logging.info(f"Plots saved in folder → {PLOT_DIR}")


# =========================================================
#   REPORT GENERATION
# =========================================================
class ReportGenerator:
    """Creates final summary report."""

    def generate(self, metrics, score):

        report = f"""
================ DATA QUALITY REPORT ================

START SHAPE: {metrics['start_shape']}
FINAL SHAPE: {metrics['final_shape']}

ROWS REMOVED TOTAL: {metrics['rows_removed_total']}
DUPLICATES REMOVED: {metrics['duplicates_removed']}

DATA QUALITY SCORE: {score}/100

PLOTS LOCATION: {PLOT_DIR}/

=====================================================
"""

        with open("data_quality_report.txt", "w") as f:
            f.write(report)

        logging.info("Report saved → data_quality_report.txt")


# =========================================================
#   PIPELINE ENGINE
# =========================================================
class DataPipeline:
    """
    Orchestrates the full ETL process:
    - Transformation
    - Validation
    - Metrics tracking
    """

    def __init__(self, transformers, validator):
        self.transformers = transformers
        self.validator = validator
        self.error_report = ErrorReport()

        self.metrics = {
            "start_shape": None,
            "final_shape": None,
            "start_rows": 0,
            "final_rows": 0,
            "rows_removed_total": 0,
            "duplicates_removed": 0
        }

    def run(self, df):

        self.metrics["start_shape"] = df.shape
        self.metrics["start_rows"] = len(df)

        logging.info(f"Pipeline started | shape: {df.shape}")

        before_dup = len(df)

        # Apply transformations sequentially
        for step in self.transformers:

            if isinstance(step, DropInvalidRows):
                df = step.transform(df, self.error_report)
            else:
                df = step.transform(df)

            logging.info(f"{step.__class__.__name__} → {df.shape}")

        # Track duplicate removal
        self.metrics["duplicates_removed"] = before_dup - len(df)

        # Validation step
        self.validator.validate(df, self.error_report)

        # Final metrics
        self.metrics["final_shape"] = df.shape
        self.metrics["final_rows"] = len(df)
        self.metrics["rows_removed_total"] = (
            self.metrics["start_rows"] - self.metrics["final_rows"]
        )

        self.error_report.metrics = self.metrics
        self.error_report.finalize()

        logging.info(f"Pipeline completed | final shape: {df.shape}")

        return df


# =========================================================
#   MAIN EXECUTION
# =========================================================
if __name__ == "__main__":

    # Load data
    source = CSVDataSource(INPUT_FILE)
    df = source.load()

    # Define pipeline steps
    transformers = [
        StandardizeValues(),
        CleanNumeric(),
        FixTransactionAmount(),
        RemoveDuplicates(),
        DropInvalidRows()
    ]

    validator = TransactionValidator()
    pipeline = DataPipeline(transformers, validator)

    # Run pipeline
    cleaned_df = pipeline.run(df)

    # Save outputs
    cleaned_df.to_csv(OUTPUT_FILE, index=False)
    pipeline.error_report.invalid_rows.to_csv(BAD_RECORDS_FILE, index=False)

    pipeline.error_report.save_audit_log()

    # Run EDA
    EDA().run(cleaned_df)

    # Compute data quality score
    scorer = DataQualityScorer()
    score = scorer.score(cleaned_df, pipeline.error_report, pipeline.metrics["start_rows"])

    print(f"\nDATA QUALITY SCORE: {score}/100")

    # Generate dashboard plots
    dashboard = DataQualityDashboard()
    dashboard.plot(
        cleaned_df,
        pipeline.metrics["start_rows"],
        pipeline.metrics["final_rows"],
        score
    )

    # Generate final report
    ReportGenerator().generate(pipeline.metrics, score)

    # Summary output
    print("\n========== FINAL SUMMARY ==========")
    print(f"Start rows: {pipeline.metrics['start_rows']}")
    print(f"Final rows: {pipeline.metrics['final_rows']}")
    print(f"Rows removed: {pipeline.metrics['rows_removed_total']}")
    print(f"Plots saved in: {PLOT_DIR}/")
    print("===================================")