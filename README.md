# Customer Transactions Data Cleaning Pipeline

## Overview
This project implements a data cleaning and transformation pipeline using Python. It processes a raw customer transactions dataset and converts it into a clean, structured, and analysis-ready format.

The goal is to identify and fix common data quality issues such as missing values, duplicates, invalid entries, and inconsistent data types.

---

## Objectives
- Load raw transaction data from a CSV file
- Identify and handle missing values
- Detect and remove duplicate records
- Validate required fields (`transaction_id`, `customer_id`, `quantity`, `unit_price`)
- Separate invalid records for auditing purposes
- Generate logs for tracking the cleaning process
- Export a clean dataset for analysis

---

## Features
- Automatic detection of missing and invalid data
- Duplicate removal
- Data type validation and correction
- Separation of bad records into a separate file
- Audit logging in JSON format
- Clean output dataset generation

---

## Technologies Used
- Python
- Pandas
- NumPy
- Logging
- JSON

---

##   Workflow
1. **Data Loading**
   - Load raw CSV dataset using Pandas

2. **Data Inspection**
   - Check dataset structure and missing values

3. **Data Cleaning**
   - Remove duplicates
   - Handle missing values
   - Validate numeric fields (e.g., quantity must be > 0)

4. **Bad Records Handling**
   - Store invalid rows separately for review

5. **Export Results**
   - Clean dataset → `customer_transactions_cleaned_dataset.csv`
   - Bad records → `bad_records.csv`
   - Logs → `audit_log.json`

---

## Project Structure
```
project/
│
├── customer_transactions_dirty_dataset.csv
├── customer_transactions_cleaned_dataset.csv
├── bad_records.csv
├── audit_log.json
├── etl_pipeline.py
└── README.md
```
---

## How to Run
```bash
pip install pandas numpy
python etl_pipeline.py
```
---
##   Output

After execution:

Clean dataset is generated for analysis
Invalid records are saved separately
Full audit trail is available in JSON logs

Python Data Cleaning Project