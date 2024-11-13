import requests
import logging
from pathlib import Path
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def read_sql_file(file_path):
    """Try different encodings to read the file"""
    encodings = ["utf-8", "utf-16", "utf-16le", "utf-16be", "ascii", "iso-8859-1"]

    for encoding in encodings:
        try:
            with open(file_path, "r", encoding=encoding) as f:
                return f.read()
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logging.error(f"Error reading with {encoding}: {str(e)}")
            continue

    logging.error(f"Failed to read file with any encoding: {file_path}")
    return None


def clean_name(name):
    """Clean SQL Server object names"""
    return name.replace("[", "").replace("]", "")


def extract_columns(sql_content):
    """Extract column names and infer types from SELECT statement"""
    try:
        # Find the SELECT statement
        select_start = sql_content.upper().find("SELECT")
        from_start = sql_content.upper().find("FROM", select_start)

        if select_start == -1 or from_start == -1:
            return None

        # Extract columns section
        columns_text = sql_content[select_start + 6 : from_start].strip()

        # Split into individual columns
        columns = [col.strip() for col in columns_text.split(",")]

        # Type mapping for common column names
        type_hints = {
            "id": "integer",
            "rate": "numeric",
            "percent": "numeric",
            "amount": "numeric",
            "date": "timestamp",
            "count": "integer",
            "flag": "boolean",
            "is": "boolean",
            "has": "boolean",
            "discount": "numeric",
            "rcc": "numeric",
            "npv": "numeric",
        }

        # Process each column
        return_columns = []
        for col in columns:
            # Remove table alias if present
            if "." in col:
                col = col.split(".")[-1]

            # Remove any brackets
            col = clean_name(col)

            # Infer type based on column name
            col_type = "text"  # default type
            col_lower = col.lower()
            for hint, type_name in type_hints.items():
                if hint in col_lower:
                    col_type = type_name
                    break

            return_columns.append(f"{col} {col_type}")

        return ",\n    ".join(return_columns)

    except Exception as e:
        logging.error(f"Error extracting columns: {str(e)}")
        return None


def convert_sql(sql_content):
    """Convert SQL Server code to PostgreSQL"""
    # Extract schema and procedure name
    schema_name = "app"  # You might want to extract this dynamically
    proc_name = (
        "GetAppraisalToolVariables"  # You might want to extract this dynamically
    )

    # Extract column information
    columns_structure = extract_columns(sql_content)
    if not columns_structure:
        logging.error("Could not extract column information")
        return None

    url = "http://127.0.0.1:11435/api/generate"

    prompt = f"""Convert this SQL Server stored procedure to PostgreSQL exactly:

Original SQL:
{sql_content}

Convert it to this exact structure:
CREATE OR REPLACE FUNCTION {schema_name}.{proc_name}()
RETURNS TABLE (
    {columns_structure}
)
LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    SELECT /* columns */ FROM /* tables */;
END;
$function$;

Rules:
1. Keep exact column names (case-sensitive)
2. Remove all brackets []
3. Use proper SQL formatting:
   - Keywords in UPPER CASE (SELECT, FROM, etc.)
   - Proper indentation
4. Remove all SQL Server specific syntax:
   - Remove: USE [Database], GO, SET statements
   - Keep: Comments and documentation
5. Always use RETURN QUERY for SELECT statements

Please provide only the converted PostgreSQL code with no explanations."""

    payload = {
        "model": "mixtral",
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.1, "top_p": 0.1, "repeat_penalty": 1.3},
    }

    try:
        response = requests.post(
            url, json=payload, timeout=60, headers={"Content-Type": "application/json"}
        )

        if response.status_code == 200:
            converted = response.json()["response"].strip()
            # Clean up any remaining brackets
            converted = converted.replace("[", "").replace("]", "")
            return converted
        else:
            logging.error(f"Error: {response.status_code} - {response.text}")
            return None

    except Exception as e:
        logging.error(f"Conversion failed: {str(e)}")
        return None


def main():
    # Test with one file first
    test_file = Path(
        "/mnt/c/Users/wpate/OneDrive - Resource Data/Documents/small-large/Small/batch1/app.GetAppraisalToolVariables.StoredProcedure.sql"
    )

    if not test_file.exists():
        logging.error(f"Test file not found: {test_file}")
        return

    # Read file with proper encoding handling
    sql_content = read_sql_file(test_file)

    if not sql_content:
        logging.error("Failed to read SQL file")
        return

    # Log original SQL
    logging.info(f"\nOriginal SQL:\n{sql_content}")

    # Try conversion
    postgres_sql = convert_sql(sql_content)

    if postgres_sql:
        logging.info("\nConverted PostgreSQL:")
        logging.info(postgres_sql)

        # Save the converted SQL
        output_dir = Path(
            "/mnt/c/Users/wpate/OneDrive - Resource Data/Documents/small-large/Small/new"
        )
        output_dir.mkdir(parents=True, exist_ok=True)

        output_file = output_dir / test_file.name.replace(
            ".StoredProcedure.sql", "_postgres.sql"
        )

        try:
            with open(output_file, "w", encoding="utf-8") as f:
                f.write("-- Original SQL Server Procedure:\n")
                f.write("/*\n")
                f.write(sql_content)
                f.write("\n*/\n\n")
                f.write(
                    "-- Converted to PostgreSQL on: "
                    + time.strftime("%Y-%m-%d %H:%M:%S")
                    + "\n\n"
                )
                f.write(postgres_sql)
            logging.info(f"\nSaved converted SQL to: {output_file}")
        except Exception as e:
            logging.error(f"Error saving file: {str(e)}")
    else:
        logging.error("Conversion failed!")


if __name__ == "__main__":
    main()
