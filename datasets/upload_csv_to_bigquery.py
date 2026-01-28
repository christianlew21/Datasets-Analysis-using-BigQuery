from google.cloud import bigquery
from dotenv import load_dotenv
import pandas as pd
import os
import json

load_dotenv()

# BigQuery Configuration
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DATASET_ID = os.getenv('DATASET_ID')

# Validate environment variables
if not PROJECT_ID or not DATASET_ID:
    raise ValueError("Missing GCP_PROJECT_ID or DATASET_ID in .env file")

# Display current Project and Dataset ID, Initialize BigQuery client
print(f"Using Project: {PROJECT_ID}, Dataset: {DATASET_ID}\n")
client = bigquery.Client()

# Create dataset if it doesn't exist
dataset_ref = client.dataset(DATASET_ID)
try:
    client.get_dataset(dataset_ref)
    print(f"Dataset {DATASET_ID} already exists.")
except Exception as e:
    print(f"Creating dataset {DATASET_ID}...")
    dataset = bigquery.Dataset(dataset_ref)

    # Set the location of the dataset in Singapore
    dataset.location = "asia-southeast1"
    client.create_dataset(dataset)
    print(f"Dataset {DATASET_ID} created.")


def infer_schema_with_overrides(csv_path, table_name, overrides_path="schema_override.json"):
    """
    Infer schema from CSV using pandas, then apply manual overrides for specific columns.
    
    Args:
        csv_path: Path to the CSV file
        table_name: Name of the table (used to lookup overrides)
        overrides_path: Path to the JSON file with schema overrides
    
    Returns:
        List of bigquery.SchemaField objects
    """
    overrides = {}
    
    print(f"Checking for overrides for {table_name}")

    try:
        with open(overrides_path, 'r') as f:
            all_overrides = json.load(f)
            overrides = all_overrides.get(table_name, {})
            if overrides:
                print(f"Found {len(overrides)} override(s) for {table_name}")
    except FileNotFoundError:
        print(f"No override file found at '{overrides_path}', using pure inference")
    except json.JSONDecodeError as e:
        print(f"Error parsing override file: {e}")

    # Read CSV to understand the data
    df = pd.read_csv(csv_path, nrows=1000)  # Sample first 1000 rows for inference
    
    # Pandas to BigQuery type mapping
    type_mapping = {
        'int64': 'INTEGER',
        'float64': 'FLOAT64',
        'object': 'STRING',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP',
    }
    
    schema = []
    overriden_columns = []

    for column_name, dtype in df.dtypes.items():
        # Check if there's an override for this column
        if column_name in overrides:
            overriden_columns.append(column_name)
            bq_type = overrides[column_name]
        else:
            # Use inferred type from pandas
            bq_type = type_mapping.get(str(dtype), 'STRING')

        schema.append(bigquery.SchemaField(column_name, bq_type))
    
    return schema

def upload_csv_to_bigquery():
    """
    Uploads all CSV files from the Cleaned Data folder to BigQuery.
    """
    # Path to folder contains cleaned CSV files
    csv_folder = "Cleaned Data"

    # Validate existance of CSV folder
    if not os.listdir(csv_folder):
        raise FileNotFoundError(f"No files found in '{csv_folder}' folder.")

    csv_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in '{csv_folder}' folder.")

    uploaded_tables = []
    failed_tables = []
    
    # Loop through all CSV files in the folder
    for filename in csv_files:
        # Remove .csv extension to get the table name
        table_name = filename[:-4]
        file_path = os.path.join(csv_folder, filename)

        # Configure the load job
        try:
            # Auto-infer schema from CSV using pandas
            schema = infer_schema_with_overrides(file_path, table_name)

            # Configure the load job with inferred schema
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                schema=schema,
                skip_leading_rows=1,  
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            # Load the CSV into BigQuery
            print(f"Loading {filename} into table {table_name}...")
            with open(file_path, "rb") as source_file:
                job = client.load_table_from_file(
                    source_file,
                    f"{PROJECT_ID}.{DATASET_ID}.{table_name}",
                    job_config=job_config,
                )

            job.result()  # Wait for the job to complete
            print(f"Successfully loaded {filename} into table {table_name}.")
            uploaded_tables.append(table_name)

        except Exception as e:
            print(f"Error loading {filename} into table {table_name}: {e}")
            failed_tables.append(table_name)
            continue # Continue to upload the next file

    print("Upload process completed.")
    print(f"Uploaded tables: {uploaded_tables}")
    print(f"Failed tables: {failed_tables}")

if __name__ == "__main__":
    upload_csv_to_bigquery()