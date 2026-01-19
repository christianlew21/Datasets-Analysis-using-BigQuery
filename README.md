# Datasets Analysis Using Big Query

![Tech](https://img.shields.io/badge/Tech-BigQuery%20%7C%20SQL-blue)

## Project Overview
This repository contains the SQL transformation logic and analytical reporting for the learning dataset. The goal of this project was to ingest raw transactional logs, normalize the data schema in **Google BigQuery**, and generate actionable insights regarding **Regional Manager Performance** and **Product Return Risks**.

## Data Model & Schema
The analysis transforms raw CSV logs into a modified **Star Schema** optimized for reporting:

* **Fact Table:** `Orders` (Transactional sales data)
* **Dimension Tables:**
    * `People` (Regional Managers - *Cleaned & Renormalized*)
    * `Returns` (Return status flags)

## Repository Structure
```text
.
├── datasets/
│   ├── Orders.csv   <-- Extracted Orders table from sample.xls in .csv format
│   ├── People.csv   <-- Extracted People table from sample.xls in .csv format
│   ├── Returns.csv  <-- Extracted Returns table from sample.xls in .csv format
│   ├── sample.xls   <-- Original dataset in .xls file format
│   └── scripts.py   <-- Python script to convert Excel sheets to CSV format

├── docs/
│   ├── analysis_report.pdf      <-- Summary of findings for the provided datasets
│   ├── DATA_FLOW_PIPELINE.md    <-- Complete data transformation flow with code & visualizations
│   └── visuals/                 <-- Charts and graph images
├── sql/
│   ├── normal_query_cleanup.sql                      <-- Database Viewing and Fixes ingestion headers (string_field_0 issue)
|   ├── people_performance_on_orders_and_returns.sql  <-- Analyze Aggregates Sales vs. Profit by Regional Manager
|   ├── return_rate_analysis.sql                      <-- Calculate Impact of Return on Actual Sales
│   └── sales_costs_profit_analysis_on_Orders.sql     <-- Visualize the actual sales, costs andf profits
└── README.md
```

### Challenges Encountered & Solutions
1. Data Ingestion & Cleaning
    - Issue: The People table was ingested with a malformed header row, resulting in generic column names (string_field_0, string_field_1) and a dirty first row.
    - Solution: Implemented a CREATE OR REPLACE strategy in sql/normal_query_cleanup.sql to:
        - Filter out the artifact header row.
        - Explicitly cast and rename columns to business entities (Regional Manager, Region).

2. Data Integrity
    - Validation: Performed LEFT JOIN checks to ensure 100% of Orders map to a valid Region in the People table.
