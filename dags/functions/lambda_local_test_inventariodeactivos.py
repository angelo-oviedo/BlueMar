import csv
import re
import pandas as pd

def clean_and_prepare_airbyte_file(input_file, output_file):
    """
    Cleans the Airbyte CSV file by:
    - Removing the first four metadata columns.
    - Standardizing headers.
    - Sanitizing numeric fields and retaining nulls for handling in Snowflake.
    """
    with open(input_file, mode='r', encoding='utf-8') as infile:
        csv_reader = csv.reader(infile)
        raw_headers = next(csv_reader)

        headers = [
            header.strip().replace(" ", "_").lower()
            for header in raw_headers[4:]
        ]

        cleaned_data = []
        for row in csv_reader:
            cleaned_row = {header: "" for header in headers}
            for idx, header in enumerate(headers):
                if idx + 4 < len(row):  
                    value = row[idx + 4].strip()
                    if "costo" in header or "total" in header or "cantidad" in header:
                        value = re.sub(r'[₡,]', '', value)
                        value = value if value.replace('.', '', 1).isdigit() else None
                    cleaned_row[header] = value if value != "" else None
            cleaned_data.append(cleaned_row)

    df = pd.DataFrame(cleaned_data)

    df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Cleaned and prepared data written to {output_file}")

input_file_path = "dags/functions/BLUEMAR - Inventario de activos.csv"
output_file_path = "dags/functions/Prepared-BLUEMAR-Inventario-de-activos.csv"

clean_and_prepare_airbyte_file(input_file_path, output_file_path)
