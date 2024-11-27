import csv
import re
import pandas as pd
from datetime import datetime

def convert_date(fecha):
    try:
        date_obj = datetime.strptime(fecha, '%d/%m/%Y')  # 15/01/2024
    except ValueError:
        return "n/a"
    return date_obj.strftime('%d/%m/%Y')


def clean_data(input_file, output_file):
    with open(input_file, mode='r', encoding='utf-8') as infile:
        csv_reader = csv.reader(infile)
        raw_headers = next(csv_reader)

        columns_to_keep = ["COSTO_POR_HORA", "FECHA_DEL_PAGO", "HORAS_TRABAJADAS",
                           "MES", "NOMBRE_DEL_COLABORADOR", "PAGO_TOTAL"]
        indices_to_keep = [raw_headers.index(col) for col in columns_to_keep]

        headers = [
            col.strip().replace(" ", "_").lower()
            for idx, col in enumerate(raw_headers) if idx in indices_to_keep
        ]

        cleaned_data = []
        for row in csv_reader:
            cleaned_row = {header: "n/a" for header in headers}
            for idx, header in zip(indices_to_keep, headers):
                if idx < len(row):
                    value = row[idx].strip()
                    value = value.lower() 

                    if header == "fecha_del_pago":
                        value = convert_date(value)

                    if header in ["costo_por_hora", "pago_total"]:
                        value = re.sub(r'[₡,]', '', value)
                        value = value if value.replace('.', '', 1).isdigit() else "n/a"

                    if header == "nombre_del_colaborador":
                        value = value.title() if value != "" else "n/a"

                    cleaned_row[header] = value if value != "" else "n/a"
            cleaned_data.append(cleaned_row)

    df = pd.DataFrame(cleaned_data)

    df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Archivo {output_file}")

input_file_path = "dags/functions/BLUEMAR-Pago-Planillas.csv"
output_file_path = "dags/functions/Prepared-BLUEMAR-Pago-Planillas.csv"

clean_data(input_file_path, output_file_path)
