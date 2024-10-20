import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from airflow_utils.utils.normalizer import normalize_column_names
from airflow_utils.utils.constants import RAW_ROOT_FOLDER, PARQUET_ROOT_FOLDER
import os
import decimal

# Define the processing function
def process_csv_to_parquet(path, filename, schema, date_columns, timestamp_columns, number_columns):
    
    file_path = f"{RAW_ROOT_FOLDER}/{path}/{filename}.csv"
    
    # Step 1: Load CSV file
    print(file_path)
    df = pd.read_csv(file_path,encoding="cp1252", sep=";",dtype=str)

    # Step 2: Normalize column headers
    normalized_columns = normalize_column_names(df.columns)
    df.rename(columns=normalized_columns, inplace=True)

    # Step 3: Adjust data types based on the schema

    # Date columns conversion
    for col, date_format in date_columns.items():
        normalized_col = normalized_columns[col]
        df[normalized_col] = pd.to_datetime(df[normalized_col], format=date_format, errors='coerce').dt.date  # converts to date

    # Timestamp columns conversion
    for col, timestamp_format in timestamp_columns.items():
        normalized_col = normalized_columns[col]
        df[normalized_col] = pd.to_datetime(df[normalized_col], format=timestamp_format, errors='coerce')  # converts to timestamp

    # Number columns conversion (Brazilian to Decimal)
    for col in number_columns:
        normalized_col = normalized_columns[col]
        df[normalized_col] = df[normalized_col].apply(lambda x: str(x).replace('.', '').replace(',', '.')).astype(str)

    # Step 4: Validate schema and convert columns to appropriate data types for Parquet
    arrow_schema = []
    for col, dtype in schema.items():
        normalized_col = normalized_columns[col]

        # Convert to proper PyArrow data types
        if dtype == "Date":
            arrow_schema.append((normalized_col, pa.date32()))
        elif dtype == "Timestamp":
            arrow_schema.append((normalized_col, pa.timestamp('s')))  # seconds precision for timestamps
        elif dtype == "Decimal":
            # Adjust precision and scale for decimals (adjust as needed, here I assume precision=18, scale=2)
            arrow_schema.append((normalized_col, pa.decimal128(precision=18, scale=2)))
        else:
            arrow_schema.append((normalized_col, pa.string()))  # Default to string if not date or decimal

    # Convert list of schema tuples to PyArrow schema
    arrow_schema = pa.schema(arrow_schema)

    # Step 5: Convert DataFrame to PyArrow Table
    # Handle Decimal conversion to proper type
    for col in number_columns:
        normalized_col = normalized_columns[col]
        df[normalized_col] = df[normalized_col].apply(lambda x: decimal_converter(x, precision=18, scale=2))  # Ensure decimal

    table = pa.Table.from_pandas(df, schema=arrow_schema)

    # Step 6: Construct the output path
    # Remove the RAW_ROOT_FOLDER path from the original file_path to get the relative path
    relative_path = os.path.relpath(file_path, RAW_ROOT_FOLDER)

    # Construct the new parquet file path, replacing the root folder with PARQUET_ROOT_FOLDER
    parquet_file_path = os.path.join(PARQUET_ROOT_FOLDER, relative_path).replace(".csv", ".parquet")

    # Ensure that the destination directory exists
    os.makedirs(os.path.dirname(parquet_file_path), exist_ok=True)

    # Step 7: Write to Parquet
    pq.write_table(table, parquet_file_path)

    # return parquet_file_path

# Decimal conversion helper function
def decimal_converter(value, precision=18, scale=2):
    try:
        return pa.scalar(decimal.Decimal(value).quantize(decimal.Decimal(10) ** -scale), type=pa.decimal128(precision, scale))
    except decimal.InvalidOperation:
        return None

# # Sample usage
# sfinge_notas_fiscais_schema = {
#     "CHAVE DE ACESSO": "String",
#     "MODELO": "String",
#     "SÉRIE": "String",
#     "NÚMERO": "String",
#     "NATUREZA DA OPERAÇÃO": "String",
#     "DATA EMISSÃO": "Date",
#     "EVENTO MAIS RECENTE": "String",
#     "DATA/HORA EVENTO MAIS RECENTE": "Timestamp",
#     "CPF/CNPJ Emitente": "String",
#     "RAZÃO SOCIAL EMITENTE": "String",
#     "INSCRIÇÃO ESTADUAL EMITENTE": "String",
#     "UF EMITENTE": "String",
#     "MUNICÍPIO EMITENTE": "String",
#     "CNPJ DESTINATÁRIO": "String",
#     "NOME DESTINATÁRIO": "String",
#     "UF DESTINATÁRIO": "String",
#     "INDICADOR IE DESTINATÁRIO": "String",
#     "DESTINO DA OPERAÇÃO": "String",
#     "CONSUMIDOR FINAL": "String",
#     "PRESENÇA DO COMPRADOR": "String",
#     "VALOR NOTA FISCAL": "Decimal"
# }

# date_columns = {
#     "DATA EMISSÃO": "dd/MM/yyyy"
# }

# timestamp_columns = {
#     "DATA/HORA EVENTO MAIS RECENTE": "dd/MM/yyyy HH:mm:ss"
# }

# number_columns = ["VALOR NOTA FISCAL"]

# # Example call (adjust file path)
# output_file = process_csv_to_parquet("sfinge_notas_fiscais.csv", sfinge_notas_fiscais_schema, date_columns, timestamp_columns, number_columns)

# print(f"Parquet file created: {output_file}")
