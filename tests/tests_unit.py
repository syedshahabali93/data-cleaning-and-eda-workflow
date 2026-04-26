import pandas as pd
import numpy as np
from main import (
    StandardizeValues,
    CleanNumeric,
    FixTransactionAmount,
    RemoveDuplicates,
    DropInvalidRows,
    TransactionValidator,
    ErrorReport
)

# ---------------------------
# FIXTURE DATA
# ---------------------------
def sample_df():
    return pd.DataFrame({
        "transaction_id": [1, 1, 2],
        "customer_id": ["A", "A", "B"],
        "quantity": ["2", "2", "-1"],
        "unit_price": ["10", "10", "5"],
        "transaction_amount": [20, 20, -5],
        "gender": ["M", "F", "m"]
    })


# ---------------------------
# TEST: Standardization
# ---------------------------
def test_standardize_values():
    df = sample_df()
    transformer = StandardizeValues()

    result = transformer.transform(df)

    assert result["gender"].iloc[0] == "male"
    assert result["gender"].iloc[1] == "female"


# ---------------------------
# TEST: Numeric Cleaning
# ---------------------------
def test_clean_numeric():
    df = sample_df()
    transformer = CleanNumeric()

    result = transformer.transform(df)

    assert result["quantity"].dtype != object
    assert pd.api.types.is_numeric_dtype(result["unit_price"])


# ---------------------------
# TEST: Fix Transaction Amount
# ---------------------------
def test_fix_transaction_amount():
    df = pd.DataFrame({
        "quantity": [2],
        "unit_price": [10]
    })

    transformer = FixTransactionAmount()
    result = transformer.transform(df)

    assert result["transaction_amount"].iloc[0] == 20


# ---------------------------
# TEST: Remove Duplicates
# ---------------------------
def test_remove_duplicates():
    df = sample_df()
    transformer = RemoveDuplicates()

    result = transformer.transform(df)

    assert len(result) == len(df.drop_duplicates())


# ---------------------------
# TEST: Drop Invalid Rows
# ---------------------------
def test_drop_invalid_rows():
    df = pd.DataFrame({
        "transaction_id": [1, 1, 2],
        "customer_id": ["A", "A", "B"],
        "quantity": [2, 2, -1],   # ✅ numeric
        "unit_price": [10, 10, 5]
    })

    error_report = ErrorReport()
    transformer = DropInvalidRows()

    result = transformer.transform(df, error_report)

    assert len(result) == 2

# ---------------------------
# TEST: Validator
# ---------------------------
def test_transaction_validator():
    df = pd.DataFrame({
        "quantity": [2],
        "unit_price": [10],
        "transaction_amount": [20]
    })

    error_report = ErrorReport()
    validator = TransactionValidator()

    validator.validate(df, error_report)

    assert len(error_report.validation_errors) == 0