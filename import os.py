import os
import time
import requests
import json
from pathlib import Path
import logging
from collections import defaultdict

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


def extract_return_structure(sql_content):
    """Extract column names and types from SELECT statement"""
    if "SELECT" not in sql_content.upper():
        return None

    # Find the main SELECT statement
    select_start = sql_content.upper().find("SELECT")
    from_start = sql_content.upper().find("FROM", select_start)

    if select_start == -1 or from_start == -1:
        return None

    # Extract columns
    columns_text = sql_content[select_start + 6 : from_start].strip()
    columns = [col.strip() for col in columns_text.split(",")]

    # Enhanced type mapping
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

        # Remove aliases
        if " AS " in col.upper():
            col = col.split(" AS ")[-1]

        # Infer type based on column name
        col_type = "text"  # default type
        col_lower = col.lower()
        for hint, type_name in type_hints.items():
            if hint in col_lower:
                col_type = type_name
                break

        return_columns.append(f"{col} {col_type}")

    return ",\n    ".join(return_columns)


def convert_to_postgres(sql_content, procedure_name):
    parts = procedure_name.replace(".StoredProcedure.sql", "").split(".")
    schema_name = parts[0]
    proc_name = parts[1]

    # Determine if it's a SELECT procedure
    is_select = "SELECT" in sql_content.upper() and "INTO" not in sql_content.upper()

    # Extract parameters
    params = extract_parameters(sql_content)
    param_text = ""
    if params:
        param_text = ",\n    ".join([f"{name} {type_str}" for name, type_str in params])

    # Define return structure
    if is_select:
        columns_structure = extract_return_structure(sql_content)
        return_structure = f"""RETURNS TABLE (
    {columns_structure}
)"""
    else:
        return_structure = "RETURNS void"

    prompt = f"""Convert this SQL Server stored procedure to PostgreSQL:

CONVERSION RULES:
1. Preserve exact table and column names (case-sensitive)
2. Keep schema prefix exactly as {schema_name}
3. For SELECT procedures:
   - Add RETURN QUERY before SELECT
   - Preserve column order exactly
4. Remove SQL Server specific syntax:
   - Remove: USE [Database], GO, SET statements
   - Keep: Comments and documentation
5. Convert data types:
   - varchar/nvarchar -> text
   - datetime -> timestamp
   - bit -> boolean
   - money/decimal -> numeric
   - varbinary -> bytea

Original SQL:
{sql_content}

Expected structure:
CREATE OR REPLACE FUNCTION {schema_name}.{proc_name}(
    {param_text}
)
{return_structure}
LANGUAGE plpgsql
AS $function$
BEGIN
    -- Your converted code here
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

        return postgres_sql

    except Exception as e:
        logging.error(f"Conversion failed for {procedure_name}: {str(e)}")
        return None


def main():
    setup_logging()

    # Define base directories
    base_dirs = [
        Path("/mnt/c/Users/wpate/OneDrive - Resource Data/Documents/small-large/Small"),
        Path("/mnt/c/Users/wpate/OneDrive - Resource Data/Documents/small-large/Large"),
    ]

    for base_dir in base_dirs:
        # Create corresponding output directory
        output_dir = base_dir.parent / f"{base_dir.name.lower()}-converted"
        output_dir.mkdir(parents=True, exist_ok=True)

        logging.info(f"\nProcessing directory: {base_dir}")
        logging.info(f"Output directory: {output_dir}")

        # Process all SQL files in directory
        sql_files = list(base_dir.rglob("*.StoredProcedure.sql"))

        for sql_file in sql_files:
            try:
                procedure_name = sql_file.name
                logging.info(f"\nProcessing {procedure_name}")

                # Read SQL content
                sql_content = read_sql_file(sql_file)
                if not sql_content:
                    continue

                # Convert to PostgreSQL
                postgres_sql = convert_to_postgres(sql_content, procedure_name)
                if not postgres_sql:
                    continue

                # Save converted SQL
                output_path = (
                    output_dir
                    / f"{procedure_name.replace('.StoredProcedure.sql', '_postgres.sql')}"
                )
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write("-- Original SQL Server Procedure:\n")
                    f.write("/*\n")
                    f.write(sql_content)
                    f.write("\n*/\n\n")
                    f.write(
                        "-- Converted to PostgreSQL on: "
                        + time.strftime("%Y-%m-%d %H:%M:%S")
                        + "\n"
                    )
                    f.write(postgres_sql)

                logging.info(f"Saved converted SQL to: {output_path}")

                # Wait between files
                time.sleep(5)

            except Exception as e:
                logging.error(f"Error processing {sql_file}: {str(e)}")
                continue


if __name__ == "__main__":
    main()
