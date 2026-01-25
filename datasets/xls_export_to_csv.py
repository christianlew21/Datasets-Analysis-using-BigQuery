import pandas as pd
import re
import os

# Path Configuration
INPUT_FILE = 'Raw Data/sample.xls'
OUTPUT_DIR = 'Cleaned Data'


def clean_column_and_sheet_names(text: str):
    """
    Standardizes strings for BigQuery: lowercase, alphanumeric + underscores only.
    """

    # Check if the text is None, if it is, return "unnamed_field"
    if text is None or str(text).strip() == "":
        return "unnamed_field"

    # Keep only letters and numbers, replace everything else with _
    cleaned_name = re.sub(r'[^a-zA-Z0-9]+', '_', str(text))

    # Lowercase all the characters, collapse underscores, and trim ends
    cleaned_name = re.sub(r'_+', '_', cleaned_name).strip('_').lower()

    # Safety check: if regex stripped everything (e.g. input was "!!!")
    if cleaned_name == "":
        return "unnamed_field"

    if cleaned_name[0].isdigit():
        cleaned_name = f"n_{cleaned_name}"

    return cleaned_name


def export_sheets_to_csv():

    """
    Loop through each sheet and save as csv
    """

    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Try to open the Excel file, throw an error if it fails
    try:
        xls_file = pd.ExcelFile(INPUT_FILE)
        sheet_names = xls_file.sheet_names
    except Exception as e:
        print(f"Failed to open {INPUT_FILE}: {e}")
        return

    completed_sheets = []
    failed_sheets = []

    for sheet_name in sheet_names:
        try:
            # Read the Excel sheet into a pandas DataFrame
            df = pd.read_excel(xls_file, sheet_name=sheet_name)

            # Check if current sheet is empty, skip if it's empty
            if df.empty:
                print(f"Skipping Sheet '{sheet_name}' as it is empty.")
                failed_sheets.append(f"{sheet_name} (empty)")
                continue

            # Clean sheet names for BigQuery compatibility
            final_sheet_name = f"{clean_column_and_sheet_names(str(sheet_name))}.csv"

            # Clean Column Names (Updating the DF directly)
            cleaned_columns = [clean_column_and_sheet_names(str(col)) for col in df.columns]

            # Handle duplicate column names (e.g., price, price -> price, price_1)
            final_columns = []
            for col in cleaned_columns:
                count = final_columns.count(col)
                final_columns.append(f"{col}_{count}" if count > 0 else col)

            df.columns = final_columns

            # Save as CSV with sheet name in Cleaned Data Folder
            csv_file_path = os.path.join(OUTPUT_DIR, final_sheet_name)
            df.to_csv(csv_file_path, index=False, encoding='utf-8')
            completed_sheets.append(sheet_name)

        # Continue to next sheet if there is an error in reading current sheet
        except Exception as e:
            print(f"Error reading sheet {sheet_name}: {e}")
            failed_sheets.append(sheet_name)
            continue

    # Print completed and failed sheets
    print("Converted sheets:", completed_sheets)
    print("Failed sheets:", failed_sheets)

if __name__ == "__main__":
    export_sheets_to_csv()
