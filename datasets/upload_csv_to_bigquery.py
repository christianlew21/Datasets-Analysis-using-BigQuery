from google.cloud import bigquery
from dotenv import load_dotenv
import os

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
            # Read the header row to set schemas for tables
            with open(file_path, "r", encoding="utf-8") as f:
                header_line = f.readline().strip()
                columns = header_line.split(",")
            
            # Define schema from header columns
            schema = [bigquery.SchemaField(col, "STRING") for col in columns]
            
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                schema=schema,
                skip_leading_rows=1,  # Skip the header row since we defined schema
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