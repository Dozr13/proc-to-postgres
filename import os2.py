import os
import re
import time
import json
from pathlib import Path
import logging
from collections import defaultdict
import requests  # Install with: pip install requests

conversion_failures = defaultdict(list)

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("conversion.log"), logging.StreamHandler()],
    )

def read_sql_file(file_path):
    """Try different encodings to read the file"""
    encodings = ["utf-8", "utf-16", "utf-16le", "utf-16be", "ascii", "iso-8859-1"]

    for encoding in encodings:
        try:
            with open(file_path, "r", encoding=encoding) as file:
                return file.read()
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logging.error(
                f"Error reading file {file_path} with encoding {encoding}: {str(e)}"
            )
            continue

    logging.error(f"Failed to read file {file_path} with any known encoding")
    return None

def clean_sql_content(sql_content):
    """Remove SQL Server specific elements before conversion"""
    lines = sql_content.split('\n')
    cleaned_lines = []

    for line in lines:
        if any(cmd in line.upper() for cmd in ['USE [', 'GO', 'SET ANSI_NULLS', 'SET QUOTED_IDENTIFIER', 'GRANT ']):
            continue
        if line.strip() == '':
            continue
        cleaned_lines.append(line)

    return '\n'.join(cleaned_lines)

def extract_schema_and_name(sql_content):
    """Extract schema and procedure name from SQL Server procedure"""
    create_proc = sql_content.upper().find('CREATE PROCEDURE')
    if create_proc == -1:
        return 'public', 'unknown_procedure'

    proc_line = sql_content[create_proc:].split('\n')[0]
    full_name = proc_line.split('[')[-2].split(']')[0] + '.' + proc_line.split('[')[-1].split(']')[0]
    schema, name = full_name.split('.')
    return schema, name

def extract_parameters(sql_content):
    """Extract original parameter names and types from SQL Server procedure"""
    params = []
    in_params = False

    for line in sql_content.split("\n"):
        line = line.strip()

        if "CREATE PROCEDURE" in line:
            in_params = True
            continue

        if in_params and "AS" in line:
            break

        if (
            in_params
            and "@" in line
            and any(
                typ in line.lower()
                for typ in ["int", "varchar", "decimal", "datetime", "bit", "varbinary"]
            )
        ):
            param = line.strip().strip(",").strip()
            name = param.split("@")[1].split(" ")[0]
            type_str = " ".join(param.split(" ")[1:]).lower()

            # Improved type mapping
            if "varbinary" in type_str:
                type_str = "bytea"
            elif "varchar" in type_str or "nvarchar" in type_str:
                type_str = "text"
            elif "decimal" in type_str or "money" in type_str:
                type_str = "numeric"
            elif "datetime" in type_str:
                type_str = "timestamp"
            elif "bit" in type_str:
                type_str = "boolean"
            elif "int" in type_str:
                type_str = "integer"

            params.append((name, type_str))

    return params

def format_numeric_type(param_type):
    """Format numeric type declarations correctly"""
    if 'numeric' in param_type:
        parts = param_type.replace('(', ' ').replace(')', '').split()
        if len(parts) == 3:
            return f"numeric({parts[1]},{parts[2]})"
    return param_type

def extract_return_structure(sql_content):
    """Extract column names and types from SELECT statement"""
    if "SELECT" not in sql_content.upper():
        return None

    select_start = sql_content.upper().find("SELECT")
    from_start = sql_content.upper().find("FROM", select_start)

    if select_start == -1 or from_start == -1:
        return None

    columns_text = sql_content[select_start + 6 : from_start].strip()
    columns = [col.strip() for col in columns_text.split(",")]

    type_hints = {
        "id": "integer",
        "date": "timestamp",
        "time": "timestamp",
        "datetime": "timestamp",
        "cost": "numeric",
        "price": "numeric",
        "amount": "numeric",
        "total": "numeric",
        "rate": "numeric",
        "percent": "numeric",
        "ratio": "numeric",
        "count": "integer",
        "number": "integer",
        "qty": "integer",
        "quantity": "integer",
        "year": "integer",
        "month": "integer",
        "day": "integer",
        "bool": "boolean",
        "flag": "boolean",
        "is": "boolean",
        "has": "boolean",
        "email": "text",
        "name": "text",
        "description": "text",
        "comment": "text",
        "address": "text",
        "phone": "text",
    }

    return_columns = []
    for col in columns:
        col = col.strip()
        if "." in col:
            col = col.split(".")[-1]

        if " AS " in col.upper():
            col = col.split(" AS ")[-1]

        col_type = "text"  # default type
        col_lower = col.lower()
        for hint, type_name in type_hints.items():
            if hint in col_lower:
                col_type = type_name
                break

        return_columns.append(f"{col} {col_type}")

    return ",\n    ".join(return_columns)

def convert_parameters_with_prefix(params, return_structure):
    """Convert parameters to use p_ prefix when they conflict with return columns"""
    if not return_structure:
        return params

    return_cols = set(col.split()[0].lower() for col in return_structure.split(',\n'))
    converted = []

    for name, type_str in params:
        if name.lower() in return_cols:
            converted.append((f"p_{name}", type_str))
        else:
            converted.append((name, type_str))

    return converted

def convert_to_postgres(sql_content, procedure_name):
    # Clean SQL content first
    clean_content = clean_sql_content(sql_content)

    # Extract schema and procedure name
    schema_name, proc_name = procedure_name.replace(".StoredProcedure.sql", "").split('.')

    # Determine if it's a SELECT procedure
    is_select = "SELECT" in clean_content.upper() and "INTO" not in clean_content.upper()

    # Extract parameters and return structure
    params = extract_parameters(clean_content)
    return_struct = extract_return_structure(clean_content) if is_select else ""

    # Convert parameters to use p_ prefix when needed
    params = convert_parameters_with_prefix(params, return_struct)
    param_text = ",\n    ".join([f"{name} {type_str}" for name, type_str in params])

    # Define return structure
    if is_select:
        return_structure = f"""RETURNS TABLE (
    {return_struct}
)"""
    else:
        return_structure = "RETURNS void"

    prompt = f"""Convert this SQL Server stored procedure to PostgreSQL following these EXACT rules:

CONVERSION RULES:
1. Keep all comments including creation date and author
2. Remove all SQL Server specific commands (USE, GO, SET, etc.)
3. Convert the procedure to a PostgreSQL function
4. For parameters:
   - Remove @ from parameter names
   - Use the exact parameter names provided
   - Format numeric types as numeric(p,s) with no spaces
5. For SELECT procedures:
   - Add RETURN QUERY before the SELECT
   - Keep exact column names and order
6. For UPDATE/INSERT:
   - Use CURRENT_TIMESTAMP instead of GETDATE()
   - Remove square brackets from identifiers

Original SQL Server procedure:
{clean_content}

Expected structure:
CREATE OR REPLACE FUNCTION {schema_name}.{proc_name}(
    {param_text}
)
{return_structure}
LANGUAGE plpgsql AS $function$
BEGIN
    -- Converted code here
END;
$function$;
"""

    try:
        response = requests.post(
            "http://localhost:11435/api/generate",
            json={
                "model": "mixtral",
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.1,
                    "num_predict": 32768,
                    "top_p": 0.1,
                    "repeat_penalty": 1.3,
                    "stop": ["```", "Human:", "Assistant:"],
                },
            },
            timeout=300,
        )
        response.raise_for_status()
        postgres_sql = response.json()["response"].strip()

        # Clean up markdown formatting
        if "```sql" in postgres_sql:
            postgres_sql = postgres_sql.split("```sql")[1].split("```")[0].strip()

        # Post-process the converted SQL
        postgres_sql = postgres_sql.replace('GETDATE()', 'CURRENT_TIMESTAMP')
        postgres_sql = postgres_sql.replace('[', '').replace(']', '')

        # Fix numeric type formatting
        for match in re.finditer(r'numeric\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)', postgres_sql):
            old = match.group(0)
            new = f"numeric({match.group(1)},{match.group(2)})"
            postgres_sql = postgres_sql.replace(old, new)

        return postgres_sql

    except Exception as e:
        logging.error(f"Conversion failed for {procedure_name}: {str(e)}")
        return None


def main():
    setup_logging()
    logging.info("Starting conversion process...")

    base_dirs = [
        Path("/mnt/c/Users/wpate/OneDrive - Resource Data/Documents/small-large/Small"),
        Path("/mnt/c/Users/wpate/OneDrive - Resource Data/Documents/small-large/Large"),
    ]

    for base_dir in base_dirs:
        # Create output directory next to input directory
        output_dir = base_dir.parent / f"{base_dir.name.lower()}-converted"
        try:
            output_dir.mkdir(parents=True, exist_ok=True)
            logging.info(f"Output directory created/verified: {output_dir}")
        except Exception as e:
            logging.error(f"Failed to create output directory {output_dir}: {e}")
            continue

        # Process batch directories
        batch_dirs = list(base_dir.glob("batch*"))
        logging.info(f"Found {len(batch_dirs)} batch directories")

        for batch_dir in batch_dirs:
            logging.info(f"\nProcessing batch directory: {batch_dir}")
            sql_files = list(batch_dir.glob("*.StoredProcedure.sql"))

            for sql_file in sql_files:
                try:
                    procedure_name = sql_file.name
                    logging.info(f"\nProcessing {procedure_name}")
                    logging.info("Original SQL:")

                    sql_content = read_sql_file(sql_file)
                    logging.info(sql_content)
                    logging.info("-" * 80)

                    if not sql_content:
                        continue

                    postgres_sql = convert_to_postgres(sql_content, procedure_name)
                    if not postgres_sql:
                        continue

                    output_path = (
                        output_dir /
                        f"{procedure_name.replace('.StoredProcedure.sql', '_postgres.sql')}"
                    )

                    # Log the actual write operation
                    logging.info(f"Writing to: {output_path}")
                    with open(output_path, 'w', encoding='utf-8') as f:
                        f.write("-- Original SQL Server Procedure:\n/*\n")
                        f.write(sql_content)
                        f.write("\n*/\n\n")
                        f.write("-- Converted to PostgreSQL on: " + time.strftime("%Y-%m-%d %H:%M:%S") + "\n")
                        f.write(postgres_sql)

                    logging.info(f"Successfully wrote: {output_path}")
                    time.sleep(2)  # Reduced wait time between files

                except Exception as e:
                    logging.error(f"Error processing {sql_file}: {str(e)}")
                    continue

            logging.info("Batch completed. Waiting 45 seconds before next batch...")
            time.sleep(45)

if __name__ == "__main__":
    main()