import xls_export_to_csv
import upload_csv_to_bigquery

if __name__ == "__main__":
    print("--- Starting Data Pipeline ---\n")
    
    print("Step 1: Converting Excel to CSV...")
    xls_export_to_csv.export_sheets_to_csv()
    
    print("\nStep 2: Uploading CSV to BigQuery...")
    upload_csv_to_bigquery.upload_all_csvs()
    
    print("\n--- Pipeline Complete ---")