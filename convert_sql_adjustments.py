import os
import time
import requests
import json
from pathlib import Path
import logging
from collections import defaultdict

conversion_failures = defaultdict(list)


def check_ollama_connection():
    try:
        # Quick check if Ollama is responding
        response = requests.get("http://localhost:11435/api/tags", timeout=5)
        if response.status_code != 200:
            return False, "Ollama server responded but returned non-200 status"

        # Test actual generation
        test_response = requests.post(
            "http://localhost:11435/api/generate",
            json={
                "model": "mixtral",
                "prompt": "test",
                "stream": False,
                "options": {"temperature": 0.1, "num_predict": 10, "top_p": 0.1},
            },
            timeout=5,
        )
        if test_response.status_code != 200:
            return False, "Ollama generate endpoint not responding correctly"

        return True, "Ollama is running and responding correctly"
    except requests.exceptions.ConnectionError:
        return False, "Could not connect to Ollama server. Is Docker container running?"
    except requests.exceptions.Timeout:
        return False, "Connection to Ollama timed out"
    except Exception as e:
        return False, f"Unexpected error checking Ollama: {str(e)}"


def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
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


def format_function_definition(sql_content, schema_name, proc_name):
    """Format the function definition to match exact requirements"""
    # Convert parameters
    params = extract_parameters(sql_content)
    param_text = (
        ", ".join([f"{name.lower()} {type_str}" for name, type_str in params])
        if params
        else ""
    )

    # Get return columns
    return_cols = extract_return_structure(sql_content)
    if return_cols:
        return_cols = ",\n    ".join(return_cols)

    # Build the function
    formatted_sql = f"""DROP FUNCTION IF EXISTS {schema_name}.{proc_name}({param_text});
CREATE OR REPLACE FUNCTION {schema_name}.{proc_name}({param_text})
    RETURNS TABLE (
    {return_cols}
    )
    STABLE
    AS $$
    BEGIN
    RETURN QUERY
    SELECT {", ".join(col.split(" ")[0] for col in return_cols.split(","))}
    FROM {schema_name}.{proc_name.split("_by")[0]}
    WHERE {params[0][0].lower()} = $1
    ORDER BY usagemonthyear;
    END;
    $$ LANGUAGE plpgsql;"""

    # Add grants if present in original
    if "GRANT" in sql_content:
        formatted_sql += f"\nGRANT SELECT ON FUNCTION {schema_name}.{proc_name}({param_text}) TO aris_web AS dbo;"

    return formatted_sql


def clean_postgres_sql(sql):
    """Clean and format PostgreSQL function definition"""
    # Remove any explanation text
    sql = "\n".join(
        line
        for line in sql.split("\n")
        if not line.startswith("Note")
        and not line.startswith("Here is")
        and not line.startswith("*")
    )

    # Remove markdown
    sql = sql.replace("```sql", "").replace("```", "").strip()

    # Format specific parts
    if "RETURNS TABLE" in sql:
        # Split into main parts
        parts = sql.split("RETURNS TABLE")
        header = parts[0].strip()
        rest = parts[1].strip()

        # Format header
        header = header.replace(
            "CREATE OR REPLACE FUNCTION", "\nCREATE OR REPLACE FUNCTION"
        )
        header = header.replace(
            "DROP FUNCTION IF EXISTS",
            "DROP FUNCTION IF EXISTS:\nDROP FUNCTION IF EXISTS",
        )

        # Format returns clause
        returns = "    RETURNS TABLE"

        # Format body
        body = rest.split("AS $$")
        returns_part = body[0].strip()
        function_body = body[1].strip()

        # Build final SQL
        sql = f"{header}{returns} ({returns_part})\n    STABLE\n    AS $$\n    {function_body}"

    return sql


def post_process_sql(sql):
    """Enforce exact formatting"""
    lines = []
    for line in sql.split("\n"):
        stripped = line.strip()

        # No indentation for CREATE line
        if stripped.startswith("CREATE OR REPLACE FUNCTION"):
            lines.append(stripped)

        # Indentation for RETURNS and other keywords
        elif stripped.startswith(
            (
                "RETURNS TABLE",
                "STABLE",
                "AS $$",
                "BEGIN",
                "END;",
                "$$ LANGUAGE plpgsql;",
            )
        ):
            lines.append("    " + stripped)

        # 8 spaces for SELECT/FROM/WHERE blocks
        elif stripped.startswith(("SELECT", "FROM", "WHERE", "ORDER BY")):
            lines.append("        " + stripped)

        # Default 4 spaces for everything else
        else:
            lines.append("    " + stripped)

    return "\n".join(lines)


import re


def convert_to_postgres(sql_content, procedure_name):
    # Add size logging and warning for very large files
    content_size = len(sql_content)
    logging.info(f"Converting {procedure_name} (size: {content_size} chars)")

    if content_size > 50000:  # Adjust threshold as needed
        logging.warning(
            f"Large file detected ({content_size} chars): {procedure_name} - conversion may take longer"
        )

    # Extract the base names for schema and procedure
    parts = procedure_name.replace(".StoredProcedure.sql", "").split(".")
    schema_name = parts[0].lower()
    proc_name = parts[1].lower().replace(".", "_")

    prompt = f"""Convert this SQL Server procedure to PostgreSQL function.
Must exactly match this format (notice the exact spaces and line breaks):

CREATE OR REPLACE FUNCTION schema.name(user_id integer)
    RETURNS void
    STABLE
    AS $$
    BEGIN
    -- SQL code
    END;
    $$ LANGUAGE plpgsql;

Rules:
1. EXACT original schema ({schema_name})
2. EXACT original table names (lowercase)
3. EXACT original column names (lowercase)
4. Proper types: bit -> boolean, int -> integer, varchar -> text
5. Use $1 for the first parameter
6. Avoid extra blank lines

Convert this SQL:
{sql_content}"""

    max_retries = 3
    current_retry = 0

    while current_retry < max_retries:
        try:
            response = requests.post(
                "http://localhost:11435/api/generate",
                json={
                    "model": "codellama",
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.1,
                        "num_predict": 32768,  # Increased
                        "num_ctx": 32768,  # Increased
                        "top_p": 0.1,
                        "repeat_last_n": 512,  # Added to help with larger files
                    },
                },
                timeout=(30, 300),  # Increased to 5 minutes
            )

            if response.status_code != 200:
                current_retry += 1
                logging.warning(f"Attempt {current_retry} failed for {procedure_name}")
                time.sleep(2)  # Wait before retry
                continue

            result = response.json()["response"].strip()

            # Check for actual SQL content
            if not (
                "CREATE OR REPLACE FUNCTION" in result or "CREATE FUNCTION" in result
            ):
                logging.warning(
                    f"No SQL found in response for {procedure_name}, retrying..."
                )
                current_retry += 1
                time.sleep(2)
                continue

            if "CREATE OR REPLACE FUNCTION" in result:
                # Rest of your existing code...
                start = result.find("CREATE OR REPLACE FUNCTION")
                end = result.rfind(";") + 1
                sql = result[start:end]

                # Apply exact formatting
                sql = post_process_sql(sql)

                # Dynamically extract parameters...
                params = extract_parameters(sql_content)
                param_map = {param[0]: f"${i+1}" for i, param in enumerate(params)}
                for param_name, pos_placeholder in param_map.items():
                    sql = re.sub(rf"\b{param_name}\b", pos_placeholder, sql)

                return sql

            return None

        except requests.exceptions.Timeout:
            current_retry += 1
            logging.warning(f"Timeout on attempt {current_retry} for {procedure_name}")
            time.sleep(5)  # Longer wait after timeout
        except Exception as e:
            logging.error(f"Error converting {procedure_name}: {str(e)}")
            return None

    logging.error(f"Failed to convert {procedure_name} after {max_retries} attempts")
    return None


def main():
    setup_logging()
    logging.info("Starting conversion...")

    base_dirs = [
        Path("/mnt/c/Users/wpate/OneDrive - Resource Data/Documents/small-large/Small"),
        Path("/mnt/c/Users/wpate/OneDrive - Resource Data/Documents/small-large/Large"),
    ]

    for base_dir in base_dirs:
        output_dir = base_dir.parent / f"{base_dir.name.lower()}-converted"
        output_dir.mkdir(parents=True, exist_ok=True)

        sql_files = list(base_dir.rglob("*.StoredProcedure.sql"))
        for index, sql_file in enumerate(sql_files, 1):
            try:
                logging.info(f"Processing {index}/{len(sql_files)}: {sql_file.name}")

                sql_content = read_sql_file(sql_file)
                if not sql_content:
                    continue

                postgres_sql = convert_to_postgres(sql_content, sql_file.name)
                if postgres_sql:
                    output_path = (
                        output_dir
                        / f"{sql_file.name.replace('.StoredProcedure.sql', '_postgres.sql')}"
                    )
                    with open(output_path, "w", encoding="utf-8") as f:
                        f.write(
                            postgres_sql.strip() + "\n"
                        )  # Ensure single trailing newline

                    logging.info(f"Converted: {sql_file.name}")
                time.sleep(0.5)

            except Exception as e:
                logging.error(f"Failed to process {sql_file}: {e}")
                continue


if __name__ == "__main__":
    main()
