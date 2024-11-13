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
            and any(typ in line.lower() for typ in ["int", "varchar", "varbinary"])
        ):
            param = line.strip().strip(",").strip()
            name = param.split("@")[1].split(" ")[0]
            type_str = param.split(" ")[-1].lower()

            if "varbinary" in type_str:
                type_str = "bytea"
            elif "varchar" in type_str or "nvarchar" in type_str:
                type_str = "text"
            elif "int" in type_str:
                type_str = "integer"

            params.append((name, type_str))

    return params


def clean_postgres_sql(postgres_sql, procedure_name):
    """Clean up and format the converted PostgreSQL code"""
    # Extract schema and procedure names first
    parts = procedure_name.replace(".StoredProcedure.sql", "").split(".")
    if len(parts) < 2:
        logging.error(f"Invalid procedure name format: {procedure_name}")
        return None
    schema_name = parts[0]
    proc_name = parts[1]

    # Process line by line for better control
    clean_lines = []
    for line in postgres_sql.split("\n"):
        if line is None:
            continue

        # Start with the line stripping
        cleaned_line = line.strip()

        # Skip empty/template lines
        if not cleaned_line or (
            cleaned_line.startswith("--")
            and ("Convert" in cleaned_line or "Replace" in cleaned_line)
        ):
            continue

        # Clean up SQL syntax - each replacement on a new line for clarity
        cleaned_line = cleaned_line.replace("dbo.", f"{schema_name}.")
        cleaned_line = cleaned_line.replace("@", "")
        cleaned_line = cleaned_line.replace("SET NOCOUNT ON", "")
        cleaned_line = cleaned_line.replace("[", "")
        cleaned_line = cleaned_line.replace("]", "")
        cleaned_line = cleaned_line.replace("getdate()", "now()")
        cleaned_line = cleaned_line.replace("PRINT", "RAISE NOTICE")
        cleaned_line = cleaned_line.replace("SELECT var = val", "SELECT val INTO var")
        cleaned_line = cleaned_line.replace("@@FETCH_STATUS = 0", "FOUND")
        cleaned_line = cleaned_line.replace("FETCH NEXT FROM", "FETCH FROM")
        cleaned_line = cleaned_line.replace("len(", "length(")
        cleaned_line = cleaned_line.replace("SCOPE_IDENTITY()", "lastval()")
        cleaned_line = cleaned_line.replace("CHAR(13)", "\n")
        cleaned_line = cleaned_line.replace("CHAR(10)", "")
        cleaned_line = cleaned_line.replace("varchar(MAX)", "text")
        cleaned_line = cleaned_line.replace("nvarchar(MAX)", "text")

        # Fix cursor syntax
        if "CURSOR FAST_FORWARD FOR" in cleaned_line:
            cleaned_line = cleaned_line.replace("CURSOR FAST_FORWARD FOR", "CURSOR FOR")

        # Fix variable assignments
        if " = " in cleaned_line and not any(
            x in cleaned_line for x in ["SELECT", "WHERE", "SET"]
        ):
            parts = cleaned_line.split(" = ")
            cleaned_line = parts[0] + " := " + parts[1]

        # Add line with proper indentation
        if cleaned_line.startswith("CREATE"):
            indent = 0
        elif any(cleaned_line.startswith(x) for x in ["DECLARE", "BEGIN", "END"]):
            indent = 4
        else:
            indent = 8

        clean_lines.append(" " * indent + cleaned_line)

    # Join all lines
    result = "\n".join(clean_lines)

    # Fix function structure
    if not result.endswith("$function$;"):
        result = result.rstrip(";") + "\n$function$;"

    if "LANGUAGE plpgsql" not in result:
        result = result.replace("AS $function$", "LANGUAGE plpgsql\nAS $function$")

    return result


def extract_return_structure(sql_content):
    """Dynamically extract column names and infer types from SELECT statement"""
    if "SELECT" not in sql_content.upper():
        return None

    # Find the SELECT statement
    select_start = sql_content.upper().find("SELECT")
    from_start = sql_content.upper().find("FROM", select_start)

    if select_start == -1 or from_start == -1:
        return None

    # Extract columns
    columns_text = sql_content[select_start + 6 : from_start].strip()
    columns = [col.strip() for col in columns_text.split(",")]

    # Define type mapping
    type_hints = {
        "id": "integer",
        "date": "timestamp",
        "datetime": "timestamp",
        "cost": "numeric",
        "price": "numeric",
        "amount": "numeric",
        "total": "numeric",
        "ratio": "numeric",
        "size": "numeric",
        "area": "numeric",
        "height": "numeric",
        "points": "integer",
        "stars": "integer",
        "count": "integer",
        "number": "integer",
        "year": "integer",
        "bool": "boolean",
        "is": "boolean",
        "mmbtu": "numeric",
    }

    # Build return structure
    return_columns = []
    for col in columns:
        col = col.strip()
        # Remove table alias if present
        if "." in col:
            col = col.split(".")[-1]

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

    # Define return structure based on procedure type
    if is_select:
        columns_structure = extract_return_structure(sql_content)
        return_structure = f"""RETURNS TABLE (
    {columns_structure}
)"""
    else:
        return_structure = "RETURNS void"

    prompt = f"""Convert this SQL Server stored procedure to PostgreSQL preserving EXACT functionality:

    RULES:
    1. For SELECT statements:
       CREATE OR REPLACE FUNCTION {schema_name}.{proc_name}({param_text})
       RETURNS TABLE (...exact columns from SELECT...)
       LANGUAGE plpgsql AS $function$
       BEGIN
         RETURN QUERY
         -- Original SELECT with Postgres syntax
       END;
       $function$

    2. For other statements:
       CREATE OR REPLACE FUNCTION {schema_name}.{proc_name}({param_text})
       RETURNS void 
       LANGUAGE plpgsql AS $function$
       BEGIN
         -- Original logic with Postgres syntax
       END;
       $function$

    Original SQL:
    {sql_content}

    Convert maintaining EXACT column names and functionality. Return ONLY the converted function."""

    # Rest of your existing function remains the same...

    # Make request to Ollama
    # Inside convert_to_postgres, right where your current request code is:
    max_retries = 3
    max_timeout_retries = 3

    for attempt in range(max_retries):
        for timeout_retry in range(max_timeout_retries):
            try:
                response = requests.post(
                    "http://127.0.0.1:11435/api/generate",
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
                break  # Break out of timeout retry loop if successful
            except requests.exceptions.Timeout:
                logging.error(f"Timeout on attempt {timeout_retry + 1} of {max_timeout_retries}")
                if timeout_retry == max_timeout_retries - 1:
                    raise
                time.sleep(30)  # Wait longer between timeout retries
            except requests.exceptions.RequestException as e:
                logging.error(f"Request failed: {str(e)}")
                break  # Break out of timeout retry loop for non-timeout errors

        try:
            postgres_sql = response.json()["response"].strip()
            # Rest of your existing code...

        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt == max_retries - 1:
                conversion_failures["api_errors"].append(procedure_name)
                return None
            time.sleep(10)

    return None


def validate_conversion(postgres_sql, procedure_name, original_sql):
    """Enhanced validation with specific checks"""
    validation_errors = []

    # Extract schema and expected table names from original SQL
    schema_name = procedure_name.split(".")[0]
    original_tables = set()
    for line in original_sql.split("\n"):
        if "FROM" in line.upper() or "UPDATE" in line.upper() or "INTO" in line.upper():
            for word in line.split():
                if "." in word:
                    table = word.strip("[].,;()")
                    if "dbo." in table:
                        table = table.replace("dbo.", f"{schema_name}.")
                    original_tables.add(table)

    # Structure checks
    if "CREATE OR REPLACE FUNCTION" not in postgres_sql:
        validation_errors.append("Missing function declaration")
    if "LANGUAGE plpgsql" not in postgres_sql:
        validation_errors.append("Missing language specification")
    if "$function$" not in postgres_sql:
        validation_errors.append("Missing function delimiter")
    if "BEGIN" not in postgres_sql:
        validation_errors.append("Missing BEGIN statement")
    if "END;" not in postgres_sql:
        validation_errors.append("Missing END statement")

    # Parameter checks
    original_params = extract_parameters(original_sql)
    for param_name, _ in original_params:
        if param_name not in postgres_sql:
            validation_errors.append(f"Missing parameter: {param_name}")

    # Table name checks
    for table in original_tables:
        if table not in postgres_sql:
            validation_errors.append(f"Missing or modified table reference: {table}")

    # SQL Server syntax that shouldn't be present
    sql_server_elements = [
        ("SET NOCOUNT ON", "Remove SET NOCOUNT ON"),
        ("GO", "Remove GO statement"),
        ("@", "Remove @ from variable names"),
        ("getdate()", "Use now() instead"),
        ("dbo.", "Use correct schema"),
        ("@@FETCH_STATUS", "Use FOUND instead"),
        ("len(", "Use length() instead"),
        ("SCOPE_IDENTITY()", "Use lastval() instead"),
        ("[", "Remove square brackets"),
        ("]", "Remove square brackets"),
        ("CHAR(13)", "Use E'\\n' instead"),
        ("CHAR(10)", "Use E'\\n' instead"),
    ]

    for element, message in sql_server_elements:
        if element in postgres_sql:
            validation_errors.append(f"Contains SQL Server syntax: {message}")

    if validation_errors:
        logging.warning(f"Validation warnings for {procedure_name}:")
        for error in validation_errors:
            logging.warning(f"- {error}")
        conversion_failures["validation_errors"].append(
            (procedure_name, validation_errors)
        )
        return False
    return True


def process_batch(batch_dir, output_dir, delay_between_files=10):
    """Process all SQL files in a batch directory with a delay between files."""
    sql_files = list(Path(batch_dir).glob("*.StoredProcedure.sql"))

    # Verify output directory exists and create if needed
    output_dir = Path(output_dir)
    if not output_dir.exists():
        logging.info(f"Creating output directory: {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        logging.info(f"Output directory exists: {output_dir}")

    for sql_file in sql_files:
        procedure_name = sql_file.name
        logging.info(f"\nProcessing {procedure_name}")

        # Read the SQL content
        sql_content = read_sql_file(sql_file)

        if sql_content is None:
            logging.error(f"Skipping {procedure_name} due to reading error")
            continue

        # Log original SQL
        logging.info(f"Original SQL:\n{sql_content}\n")
        logging.info("-" * 80)  # Separator

        # Convert to PostgreSQL
        postgres_sql = convert_to_postgres(sql_content, procedure_name)

        if postgres_sql:
            # Log the converted SQL
            logging.info(f"Converted SQL:\n{postgres_sql}\n")
            logging.info("-" * 80)  # Separator

            # Create output filename
            output_filename = procedure_name.replace(
                ".StoredProcedure.sql", "_postgres.sql"
            )
            output_path = output_dir / output_filename

            # Save both original and converted SQL
            try:
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
                    f.write(
                        "-- WARNING: Please test thoroughly before using in production\n\n"
                    )
                    f.write(postgres_sql)
                logging.info(f"Saved converted SQL to: {output_path}")
            except Exception as e:
                logging.error(
                    f"Error saving converted SQL for {procedure_name}: {str(e)}"
                )
                logging.error(f"Attempted to save to: {output_path}")

            # Validate after saving
            if validate_conversion(postgres_sql, procedure_name, sql_content):
                logging.info(f"Validation passed for {procedure_name}")
            else:
                logging.error(
                    f"Validation failed for {procedure_name} but file was saved for review"
                )

        # Delay between files to prevent overload
        logging.info(f"Waiting {delay_between_files} seconds before next file...")
        time.sleep(delay_between_files)


def main():
    # Add timing
    start_time = time.time()

    try:
        # Change paths for new directory structure
        base_dir = Path(
            "/mnt/c/Users/wpate/OneDrive - Resource Data/Documents/small-large/Small"
        )
        output_dir = Path(
            "/mnt/c/Users/wpate/OneDrive - Resource Data/Documents/small-large/small-converted"
        )

        # Verify base directory exists
        if not base_dir.exists():
            logging.error(f"Base directory does not exist: {base_dir}")
            return

        # Setup logging
        setup_logging()

        logging.info(f"Base directory: {base_dir}")
        logging.info(f"Output directory will be: {output_dir}")

        # Create output directory if it doesn't exist
        output_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"Output directory status - exists: {output_dir.exists()}")

        logging.info("Starting conversion process...")
        logging.info(
            "Make sure Ollama is running with the latest model for best results"
        )

        # Process each batch directory
        batch_dirs = [
            d for d in base_dir.iterdir() if d.is_dir() and d.name.startswith("batch")
        ]
        logging.info(f"Found {len(batch_dirs)} batch directories")

        for batch_dir in sorted(batch_dirs):
            try:
                logging.info(f"\nProcessing batch directory: {batch_dir}")
                process_batch(batch_dir, output_dir)
                logging.info("Batch completed. Waiting 45 seconds before next batch...")
                time.sleep(45)
            except KeyboardInterrupt:
                logging.info(
                    "\nInterrupted by user during batch processing. Cleaning up..."
                )
                break
            except Exception as e:
                logging.error(f"Error processing batch {batch_dir}: {str(e)}")
                continue

    except KeyboardInterrupt:
        logging.info("\nInterrupted by user. Shutting down...")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
    finally:
        # Calculate and display elapsed time
        elapsed_time = time.time() - start_time
        hours = int(elapsed_time // 3600)
        minutes = int((elapsed_time % 3600) // 60)
        seconds = int(elapsed_time % 60)

        logging.info("\nConversion Statistics:")
        logging.info(f"Total time elapsed: {hours}h {minutes}m {seconds}s")
        logging.info(f"Total batch directories processed: {len(batch_dirs)}")
        if conversion_failures:
            logging.info("\nFailures encountered:")
            for error_type, failures in conversion_failures.items():
                logging.info(f"{error_type}: {len(failures)} failures")


if __name__ == "__main__":
    main()
