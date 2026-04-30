import pandas as pd
from main import (
    DataPipeline,
    StandardizeValues,
    CleanNumeric,
    FixTransactionAmount,
    RemoveDuplicates,
    DropInvalidRows,
    TransactionValidator
)

# ---------------------------
# SAMPLE DATA
# ---------------------------
def test_full_pipeline():

    df = pd.DataFrame({
        "transaction_id": [1, 2, 2],
        "customer_id": ["A", "B", "B"],
        "quantity": [2, 3, -1],
        "unit_price": [10, 5, 5],
        "transaction_amount": [20, 15, -5],
        "gender": ["m", "f", "m"],
        "product_category": ["electronics", "clothing", "clothing"]
    })

    transformers = [
        StandardizeValues(),
        CleanNumeric(),
        FixTransactionAmount(),
        RemoveDuplicates(),
        DropInvalidRows()
    ]

    validator = TransactionValidator()
    pipeline = DataPipeline(transformers, validator)

    result = pipeline.run(df)

    # ---------------------------
    # ASSERTIONS
    # ---------------------------
    assert isinstance(result, pd.DataFrame)
    assert "transaction_amount" in result.columns
    assert result["quantity"].min() > 0  # invalid rows removed
    assert pipeline.metrics["start_rows"] > pipeline.metrics["final_rows"]