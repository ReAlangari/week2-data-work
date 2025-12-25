# ETL Project — Orders & Users Analytics

## 📋 Project Overview
This project demonstrates a full **ETL (Extract, Transform, Load)** workflow for e-commerce order and user data. The goal is to clean, enrich, and aggregate raw CSV data into a structured analytics table suitable for analysis and visualization.

## 📁 Raw Data
Two CSV files are used as input:

- **`orders.csv`** — contains individual order records (order ID, user ID, amount, quantity, status, timestamps)
- **`users.csv`** — contains user information (user ID, country, signup date)

## 🔄 ETL Pipeline
The pipeline is implemented in three main stages:

### **1. Load & Convert** (`etl.py` - Load Stage)
- Reads raw CSVs from the `data/raw/` directory
- Converts data to Parquet format for faster processing
- Ensures basic schema enforcement on the orders dataset
- Saves intermediate outputs:
  - `orders.parquet`
  - `users.parquet`
- Writes a `_run_meta.json` metadata file containing row counts and timestamps

### **2. Data Cleaning** (`etl.py` - Transform Stage)
Performs validations and quality checks:
- Required columns are present
- No empty datasets
- Unique keys for users and orders
- Values within expected ranges (e.g., amount ≥ 0, quantity ≥ 1)
- Cleans and normalizes categorical fields (status column mapped to standard categories: `paid` and `refund`)
- Deduplicates orders, keeping the latest record per `order_id`
- Generates missingness flags for tracking data quality

### **3. Analytics Table** (`etl.py` - Transform Stage)
- Parses timestamps and adds derived time features (year, month, day of week, hour)
- Joins orders with user information to enrich analytics data
- Computes winsorized amounts to reduce outlier effects
- Flags extreme values for further analysis
- Produces the final analytics table: `analytics_table.parquet`

## 📊 Output Files
All processed data and metadata are stored in the `data/processed/` directory:

| File | Description |
|------|-------------|
| `orders_clean.parquet` | Cleaned order records with validated schema |
| `users.parquet` | User information table |
| `analytics_table.parquet` | **Final analytics-ready table** with enriched features |
| `_run_meta.json` | Pipeline metadata with timestamps, row counts, and quality metrics |

## 🚀 How to Run

### **Option 1: Full Pipeline**
```bash
python src/bootcamp_data/scripts/run_etl.py