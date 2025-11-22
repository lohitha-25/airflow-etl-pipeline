# Transaction Data ETL Pipeline using Apache Airflow

## Overview

This project is an end-to-end **ETL data pipeline** built using **Apache Airflow** and **Docker** to process financial transaction data.  
The pipeline reads raw transactions from a CSV file, cleans the data, adds a simple fraud flag, and generates an analytics summary.

## Tech Stack

- **Apache Airflow** (workflow orchestration)
- **Docker & Docker Compose**
- **Python**, **pandas**
- **PostgreSQL**, **Redis** (used internally by Airflow)
- **CSV files** for input/output

---

## Architecture

```mermaid
flowchart LR
    A[Raw CSV: transactions.csv<br/>data/raw] --> B[Airflow DAG: transaction_data_pipeline]
    B --> C[Clean Data: transactions_clean.csv<br/>data/clean]
    C --> D[Transform: Add is_suspicious flag]
    D --> E[Analytics Output: fraud_summary.csv<br/>data/analytics]
