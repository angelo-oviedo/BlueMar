import csv
import re
import pandas as pd
import os
from datetime import datetime
from io import StringIO

# Directories for local testing
LOCAL_INPUT_DIR = "dags/functions/local-input"
LOCAL_OUTPUT_DIR = "dags/functions/local-output"

def convert_date(fecha, formats=['%d-%m-%y', '%d/%m/%y', '%d/%m/%Y']):
    """
    Attempts to convert a date string to a standardized format.
    Returns None if the date is invalid.
    """
    for fmt in formats:
        try:
            return datetime.strptime(fecha, fmt).strftime('%d/%m/%Y')
        except ValueError:
            continue
    return None

def clean_value(value):
    """
    Cleans a single cell value by removing unwanted symbols and cleaning numeric values.
    """
    if value.lower() in ('n/a', 'null', ''):
        return None
    # Remove '₡' symbol and commas
    value = re.sub(r'[₡,]', '', value).strip()
    return value

def process_csv(input_file, columns_to_keep, numeric_cols, date_cols, title_cols):
    """
    Process the CSV content and clean the data.
    """
    csv_reader = csv.reader(StringIO(input_file))
    raw_headers = next(csv_reader)

    # Clean and standardize column names for output
    indices_to_keep = [raw_headers.index(col) for col in columns_to_keep]
    headers = [col.strip().replace(" ", "_").lower().rstrip("_") for col in columns_to_keep]

    cleaned_data = []
    for row in csv_reader:
        cleaned_row = {header: None for header in headers}
        for idx, header in zip(indices_to_keep, headers):
            if idx < len(row):
                value = row[idx].strip()
                value = clean_value(value)

                if value is not None:
                    if header in date_cols:
                        value = convert_date(value)

                    if header in numeric_cols:
                        # Ensure numeric validity
                        value = value if value.replace('.', '', 1).isdigit() else None

                    if header in title_cols:
                        value = value.title() if value else None

                cleaned_row[header] = value
        cleaned_data.append(cleaned_row)

    df = pd.DataFrame(cleaned_data)
    output_csv = StringIO()
    df.to_csv(output_csv, index=False, encoding='utf-8')
    output_csv.seek(0)

    return output_csv.getvalue()

def clean_and_save(file_type, input_filename, output_filename, columns_to_keep, numeric_cols, date_cols, title_cols):
    """
    Main function to clean a specific file type for local testing.
    """
    try:
        input_path = os.path.join(LOCAL_INPUT_DIR, input_filename)
        with open(input_path, mode='r', encoding='utf-8') as infile:
            raw_csv = infile.read()

        # Process the file
        cleaned_csv = process_csv(raw_csv, columns_to_keep, numeric_cols, date_cols, title_cols)

        # Save the processed file locally
        output_path = os.path.join(LOCAL_OUTPUT_DIR, output_filename)
        with open(output_path, mode='w', encoding='utf-8') as outfile:
            outfile.write(cleaned_csv)
        print(f"Processed file saved at {output_path}")

    except Exception as e:
        print(f"Error processing {file_type}: {str(e)}")

def main():
    """
    Main function to process all file types.
    """
    os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)  # Ensure output directory exists

    cleaning_jobs = [
        ("Gastos_operativos", "gastos-operativos.csv", "processed_gastos_operativos.csv",
        ["cantidad_", "concepto_", "descripcion_", "estado_factura", "factura", "fecha", "fecha_de_pago", "mes", "monto_unitario", "proveedor_", "referencia_banco", "total"],
        ["cantidad_", "monto_unitario", "total"], ["fecha", "fecha_de_pago"], ["proveedor_"]),
        
        ("Ingresos", "ingresos.csv", "processed_ingresos.csv",
        ["cantidad", "cliente", "estado_factura", "factura", "fecha", "mes", "precio_unitario", "referencia_banco", "talla", "total"],
        ["precio_unitario", "total"], ["fecha"], ["cliente"]),

        ("Inventario_de_activos", "inventario-de-activos.csv", "processed_inventario_de_activos.csv",
        ["activo", "ano_de_adquisicion", "cantidad_", "costo", "fuente_de_financiamiento", "tipo", "total"],
        ["cantidad_", "costo", "total"], [], ["activo", "tipo", "fuente_de_financiamiento"]),
        
        ("Pago_planillas", "pago-planillas.csv", "processed_pago_planillas.csv",
        ["costo_por_hora", "fecha_del_pago", "horas_trabajadas", "mes", "nombre_del_colaborador", "pago_total"],
        ["costo_por_hora", "pago_total"], ["fecha_del_pago"], ["nombre_del_colaborador"]),
    ]

    for file_type, input_filename, output_filename, cols_to_keep, numeric_cols, date_cols, title_cols in cleaning_jobs:
        print(f"Processing {file_type}")
        clean_and_save(file_type, input_filename, output_filename, cols_to_keep, numeric_cols, date_cols, title_cols)

if __name__ == "__main__":
    main()