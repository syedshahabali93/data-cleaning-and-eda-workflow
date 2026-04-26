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
