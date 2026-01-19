import pandas as pd

# Read the Excel file
xls_file = pd.ExcelFile('sample.xls')

sheet_names = xls_file.sheet_names

# Loop through each sheet and save as csv
for sheet_name in sheet_names:
    df = pd.read_excel(xls_file, sheet_name=sheet_name)
    
    # Save as CSV with sheet name
    output_filename = f'{sheet_name}.csv'
    df.to_csv(output_filename, index=False, encoding='utf-8')
    