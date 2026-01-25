# Datasets Analysis Using BigQuery

![Tech](https://img.shields.io/badge/Tech-BigQuery%20%7C%20SQL%20%7C%20Python-blue)

> A complete data pipeline from raw Excel files to actionable business insights using **Python**, **Google BigQuery**, and **SQL**.

---

## Project Overview

This repository contains the complete ETL pipeline and analytical reporting for a sales dataset. The goal is to ingest raw transactional logs, normalize the data schema in **Google BigQuery**, and generate actionable insights regarding **Regional Manager Performance** and **Product Return Risks**.

---

## Pipeline Overview

```mermaid
flowchart LR
    subgraph Source["Source Data"]
        A["sample.xls"]
    end
    
    subgraph Python["Python Pipeline"]
        direction TB
        M["main.py"]
        B["xls_export_to_csv.py"]
        U["upload_csv_to_bigquery.py"]
        M --> B
        M --> U
    end
    
    subgraph CSVs["CSV Files (Cleaned Data)"]
        C1["orders.csv"]
        C2["people.csv"]
        C3["returns.csv"]
    end
    
    subgraph BQ["Google BigQuery"]
        direction TB
        D0["Dataset Creation"]
        D1["Data Cleanup"]
        D2["Sales Analysis"]
        D3["Performance Analysis"]
        D4["Return Rate Analysis"]
        D0 --> D1
        D1 --> D2
        D1 --> D3
        D1 --> D4
    end
    
    subgraph Output["Insights"]
        E["Visualizations & Reports"]
    end
    
    A --> B
    B --> C1
    B --> C2
    B --> C3
    C1 --> U
    C2 --> U
    C3 --> U
    U --> D0
    D2 --> E
    D3 --> E
    D4 --> E
```

---

## Tech Stack

| Technology | Purpose |
|------------|---------|
| **Python** (pandas, xlrd) | Data extraction & CSV conversion |
| **Python** (google-cloud-bigquery) | Automated BigQuery upload |
| **Python** (python-dotenv) | Environment variable management |
| **Google BigQuery** | Cloud data warehouse |
| **SQL** | Data transformation & analysis |

---

## Repository Structure

```text
.
├── credentials/                    <-- GCP service account credentials (gitignored)
├── datasets/
│   ├── Raw Data/
│   │   └── sample.xls              <-- Original multi-sheet Excel dataset
│   ├── Cleaned Data/
│   │   ├── orders.csv              <-- Extracted Orders table (~9,000+ rows)
│   │   ├── people.csv              <-- Extracted People table (4 rows)
│   │   └── returns.csv             <-- Extracted Returns table (~300 rows)
│   ├── main.py                     <-- Entry point script (orchestrates pipeline)
│   ├── xls_export_to_csv.py        <-- Excel to CSV conversion with column cleaning
│   ├── upload_csv_to_bigquery.py   <-- BigQuery upload automation
│   ├── requirements.txt            <-- Python dependencies
│   └── .env                        <-- Environment configuration (GCP_PROJECT_ID, DATASET_ID)
├── docs/
│   ├── analysis_report.pdf         <-- Summary of findings
│   └── visuals/                    <-- Chart visualizations (25 images)
├── sql/
│   ├── normal_query_cleanup.sql
│   ├── people_performance_on_orders_and_returns.sql
│   ├── return_rate_analysis.sql
│   └── sales_costs_profit_analysis_on_Orders.sql
├── .gitignore
└── README.md
```

---

## Data Model & Schema

The analysis transforms raw CSV logs into a modified **Star Schema** optimized for reporting:

* **Fact Table:** `Orders` (Transactional sales data)
* **Dimension Tables:**
    * `People` (Regional Managers - *Cleaned & Renormalized*)
    * `Returns` (Return status flags)

---

## Step 1: Data Extraction (Python)

**Purpose:** Convert multi-sheet Excel file into separate CSV files with cleaned column names for BigQuery compatibility.

**Script:** [`datasets/xls_export_to_csv.py`](datasets/xls_export_to_csv.py)

```python
import pandas as pd
import re
import os

def clean_column_and_sheet_names(text: str):
    """Standardizes strings for BigQuery: lowercase, alphanumeric + underscores only."""
    if text is None or str(text).strip() == "":
        return "unnamed_field"
    cleaned_name = re.sub(r'[^a-zA-Z0-9]+', '_', str(text))
    cleaned_name = re.sub(r'_+', '_', cleaned_name).strip('_').lower()
    if cleaned_name == "":
        return "unnamed_field"
    if cleaned_name[0].isdigit():
        cleaned_name = f"n_{cleaned_name}"
    return cleaned_name

def export_sheets_to_csv():
    """Loop through each Excel sheet and save as cleaned CSV."""
    xls_file = pd.ExcelFile('Raw Data/sample.xls')
    for sheet_name in xls_file.sheet_names:
        df = pd.read_excel(xls_file, sheet_name=sheet_name)
        df.columns = [clean_column_and_sheet_names(col) for col in df.columns]
        output_filename = f'Cleaned Data/{clean_column_and_sheet_names(sheet_name)}.csv'
        df.to_csv(output_filename, index=False, encoding='utf-8')
```

**Output:** 3 CSV files extracted from Excel sheets:

| File | Description | Records |
|------|-------------|---------|
| Orders.csv | Transactional sales data | ~9,000+ rows |
| People.csv | Regional Manager assignments | 4 rows |
| Returns.csv | Return status flags | ~300 rows |

---

## Step 1.5: BigQuery Upload (Python)

**Purpose:** Automatically create BigQuery dataset and upload all cleaned CSV files.

**Script:** [`datasets/upload_csv_to_bigquery.py`](datasets/upload_csv_to_bigquery.py)

```python
from google.cloud import bigquery
from dotenv import load_dotenv
import os

load_dotenv()

# BigQuery Configuration from .env
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DATASET_ID = os.getenv('DATASET_ID')

client = bigquery.Client()

def upload_csv_to_bigquery():
    """Uploads all CSV files from the Cleaned Data folder to BigQuery."""
    csv_folder = "Cleaned Data"
    for filename in os.listdir(csv_folder):
        if filename.endswith('.csv'):
            table_name = filename[:-4]
            # Configure and execute load job with auto-schema detection
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            # Load CSV to BigQuery table
            client.load_table_from_file(file, f"{PROJECT_ID}.{DATASET_ID}.{table_name}")
```

**Features:**
- Auto-creates dataset if it doesn't exist (Singapore region: `asia-southeast1`)
- Schema detection from CSV headers
- Truncates existing tables on re-upload

---

## Step 2: Data Ingestion & Cleanup (SQL)

**Purpose:** Validate data quality and fix ingestion issues in BigQuery.

**Script:** [`sql/normal_query_cleanup.sql`](sql/normal_query_cleanup.sql)

### 2.1 Preview & Validate Tables
```sql
-- Row Counts for All Tables
SELECT 'orders' as table_name, COUNT(*) as row_count FROM `test.Orders`
UNION ALL
SELECT 'people', COUNT(*) FROM `test.People`
UNION ALL
SELECT 'returns', COUNT(*) FROM `test.Returns`;
```

### 2.2 Check for Null Values & Duplicates
```sql
-- Check null value on columns of Orders Table
SELECT 
  COUNTIF('Order ID' IS NULL) as null_order_ids,
  COUNTIF('Customer ID' IS NULL) as null_customer_ids,
  COUNTIF('Product ID' IS NULL) as null_product_ids
FROM `test.Orders`;

-- Check for duplicates in primary keys
SELECT `Order ID` as order_id, COUNT(*) as cnt
FROM `test.Orders`
GROUP BY order_id
HAVING cnt > 1;
```

### 2.3 Fix Malformed Headers (People Table)
```sql
-- Rename Column Names of People (fix ingestion issue)
CREATE OR REPLACE TABLE `test.People` AS
SELECT
  string_field_0 AS `Regional Manager`,
  string_field_1 AS Region
FROM `test.People`
WHERE string_field_0 != `Regional Manager` 
  AND string_field_0 IS NOT NULL;
```

> **Note:** The People table was ingested with generic column names (`string_field_0`, `string_field_1`) due to malformed headers. This was resolved using a CREATE OR REPLACE strategy.

---

## Step 3: Sales, Costs & Profit Analysis (SQL)

**Purpose:** Analyze revenue trends, regional performance, and profitability metrics.

**Script:** [`sql/sales_costs_profit_analysis_on_Orders.sql`](sql/sales_costs_profit_analysis_on_Orders.sql)

### 3.1 Monthly Revenue Trends
```sql
SELECT 
  FORMAT_DATE('%Y-%m', `Order Date`) as month,
  COUNT(*) as order_count,
  SUM(Sales) as monthly_revenue,
  AVG(Sales) as avg_order_value
FROM `test.Orders`
GROUP BY month
ORDER BY month;
```

### 3.2 Regional Sales Performance
```sql
SELECT 
  Region,
  SUM(Sales) as total_sales,
  AVG(Sales) as average_order_value
FROM `test.Orders`
GROUP BY Region
ORDER BY total_sales DESC;
```

### 3.3 Total Costs vs Profit
```sql
SELECT 
  SUM(Sales) as total_sales,
  SUM(Profit) as total_profit,
  SUM(Sales) - SUM(Profit) as total_costs
FROM `test.Orders`;
```

**Key Visualizations:**

![Monthly Revenue Trend](docs/visuals/monthly_revenue%20by%20month.png)

![Sales by Region](docs/visuals/total_sales%20by%20Region.png)

---

## Step 4: Regional Manager Performance (SQL)

**Purpose:** Evaluate sales performance and profit margins by Regional Manager.

**Script:** [`sql/people_performance_on_orders_and_returns.sql`](sql/people_performance_on_orders_and_returns.sql)

### 4.1 Sales Performance by Manager
```sql
SELECT
  p.`Regional Manager` AS Regional_Manager,
  p.Region,
  ROUND(SUM(o.Sales), 2) AS Total_Sales,
  ROUND(SUM(o.Profit), 2) AS Total_Profit,
  COUNT(DISTINCT o.`Order ID`) AS Total_Orders,
  ROUND((SUM(o.Profit) / SUM(o.Sales)) * 100, 2) AS Profit_Margin_Pct
FROM `test.Orders` AS o
INNER JOIN `test.People` AS p ON o.Region = p.Region
GROUP BY 1, 2
ORDER BY Total_Profit DESC;
```

### 4.2 Data Integrity Check
```sql
-- Verify all orders have assigned managers
SELECT o.Region AS Order_Region, COUNT(o.`Order ID`) AS Orphaned_Orders
FROM `test.Orders` AS o
LEFT JOIN `test.People` AS p ON o.Region = p.Region
WHERE p.`Regional Manager` IS NULL
GROUP BY 1;
```

**Key Visualizations:**

![Manager Performance](docs/visuals/Total_Sales,%20Total_Profit%20by%20Regional_Manager.png)

![Profit Margin by Manager](docs/visuals/Profit_Margin_Pct%20by%20Regional_Manager.png)

---

## Step 5: Return Rate Analysis (SQL)

**Purpose:** Analyze product returns and their impact on profitability.

**Script:** [`sql/return_rate_analysis.sql`](sql/return_rate_analysis.sql)

### 5.1 Return Rate by Ship Mode
```sql
SELECT 
  o.`Ship Mode`,
  COUNT(DISTINCT o.`Order ID`) as total_orders,
  COUNT(DISTINCT r.`Order ID`) as returned_orders,
  ROUND(COUNT(DISTINCT r.`Order ID`) * 100.0 / COUNT(DISTINCT o.`Order ID`), 2) as return_rate_pct,
  SUM(o.Sales) - SUM(CASE WHEN r.Returned IS TRUE THEN o.Sales ELSE 0 END) as total_sales_after_return
FROM `test.Orders` o
LEFT JOIN `test.Returns` r ON o.`Order ID` = r.`Order ID`
GROUP BY o.`Ship Mode`
ORDER BY return_rate_pct DESC;
```

### 5.2 Profitability Impact by Category
```sql
SELECT 
  o.Category,
  SUM(o.Profit) as total_profit,
  SUM(CASE WHEN r.Returned IS TRUE THEN o.Profit ELSE 0 END) as lost_profit_due_to_return,
  ROUND(SUM(CASE WHEN r.Returned IS TRUE THEN o.Profit ELSE 0 END) * 100.0 / 
        NULLIF(SUM(o.Profit), 0), 2) as profit_loss_pct
FROM `test.Orders` o
LEFT JOIN `test.Returns` r ON o.`Order ID` = r.`Order ID`
GROUP BY o.Category
ORDER BY lost_profit_due_to_return DESC;
```

**Key Visualizations:**

![Return Rate by Category](docs/visuals/return_rate_pct%20by%20Category.png)

![Profit Loss from Returns](docs/visuals/total_profit,%20lost_profit_due_to_return%20by%20Category.png)

---

## Key Insights

| Metric | Insight |
|--------|---------|
| **Data Quality** | 100% of orders mapped to valid regions after cleanup |
| **Regional Performance** | Significant variation in profit margins across managers |
| **Return Impact** | Product returns affect category profitability differently |
| **Monthly Trends** | Clear seasonal patterns in order volume and revenue |

---

## Getting Started

### Prerequisites
- Python 3.8+
- Google Cloud Platform account with BigQuery enabled
- Service account credentials with BigQuery access

### Setup
1. Clone the repository
2. Install dependencies:
   ```bash
   cd datasets
   pip install -r requirements.txt
   ```
3. Configure environment variables in `.env`:
   ```
   GCP_PROJECT_ID=your-gcp-project-id
   DATASET_ID=your-bigquery-dataset-name
   ```
4. Set up Google Cloud authentication:
   ```bash
   gcloud auth application-default login
   ```
   Or place your GCP service account JSON in the `credentials/` folder and set:
   ```
   GOOGLE_APPLICATION_CREDENTIALS=credentials/your-service-account.json
   ```

### Running the Pipeline

**Option 1: Run full pipeline (recommended)**
```bash
cd datasets
python main.py
```

**Option 2: Run individual steps**
1. **Extract CSV files from Excel:**
   ```bash
   python xls_export_to_csv.py
   ```
2. **Upload CSVs to BigQuery:**
   ```bash
   python upload_csv_to_bigquery.py
   ```
3. **Run SQL analysis scripts** in BigQuery console

---

## Challenges Encountered & Solutions

1. **Data Ingestion & Cleaning**
    - **Issue:** The People table was ingested with a malformed header row, resulting in generic column names (`string_field_0`, `string_field_1`) and a dirty first row.
    - **Solution:** Implemented a `CREATE OR REPLACE` strategy in `sql/normal_query_cleanup.sql` to filter out the artifact header row and explicitly cast and rename columns to business entities (`Regional Manager`, `Region`).

2. **Data Integrity**
    - **Validation:** Performed `LEFT JOIN` checks to ensure 100% of Orders map to a valid Region in the People table.
