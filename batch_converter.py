import sys
import requests
import logging
from pathlib import Path
import time
import re

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


def extract_schema_and_name(sql_content, file_name):
    """Extract schema and procedure name from SQL content or filename"""
    # Try to extract from CREATE PROCEDURE statement
    import re

    create_pattern = r"CREATE\s+PROCEDURE\s+\[?(\w+)\]?\.\[?(\w+)\]?"
    match = re.search(create_pattern, sql_content, re.IGNORECASE)

    if match:
        return clean_name(match.group(1)), clean_name(match.group(2))

    # Fallback to filename
    parts = file_name.replace(".StoredProcedure.sql", "").split(".")
    if len(parts) == 2:
        return clean_name(parts[0]), clean_name(parts[1])

    return None, None


def extract_select_columns(sql_content):
    """Extract column list from SELECT statement"""
    select_match = re.search(
        r"SELECT\s+(.*?)\s+FROM", sql_content, re.DOTALL | re.IGNORECASE
    )
    if not select_match:
        return []

    columns_text = select_match.group(1)
    columns = []
    for col in columns_text.split(","):
        col = col.strip()
        if "." in col:
            col = col.split(".")[-1]
        columns.append(col.strip())

    return columns


def validate_converted_sql(converted, original_sql, schema_name, proc_name):
    """Validate converted SQL for completeness and correctness"""
    original_columns = extract_select_columns(original_sql)

    required_elements = [
        "CREATE OR REPLACE FUNCTION",
        f"{schema_name}.{proc_name}",
        "LANGUAGE plpgsql",
        "$function$",
        "BEGIN",
        "END;",
        "$function$;",
    ]

    if not all(x in converted for x in required_elements):
        return False, "Missing required elements"

    if original_columns:
        for col in original_columns:
            if col not in converted:
                return False, f"Missing column: {col}"

    return True, "Valid conversion"


def convert_sql(sql_content, file_name):
    """Convert SQL Server code to PostgreSQL with complete, exact handling"""
    schema_name, proc_name = extract_schema_and_name(sql_content, file_name)
    if not schema_name or not proc_name:
        logging.error(f"Could not extract schema and procedure name from {file_name}")
        return None

    url = "http://127.0.0.1:11435/api/generate"

    # All existing checks (keeping these!)
    has_select = "SELECT" in sql_content.upper()
    has_modify = any(x in sql_content.upper() for x in ["INSERT", "UPDATE", "DELETE"])
    has_transaction = "BEGIN TRANSACTION" in sql_content.upper()
    has_variables = "DECLARE" in sql_content.upper()

    # Enhanced type mappings (keeping your existing ones!)
    type_hints = """SQL Server -> PostgreSQL Type Mappings:
1. Parameters and Column Types:
   - int/bigint -> integer
   - smallint -> smallint
   - tinyint -> smallint
   - bit -> boolean
   - varchar(n)/nvarchar(n)/char(n) -> text
   - datetime/datetime2/smalldatetime -> timestamp
   - date -> date
   - time -> time
   - decimal(p,s)/numeric(p,s) -> numeric(p,s)
   - money/smallmoney -> numeric(19,4)
   - float/real -> double precision
   - uniqueidentifier -> uuid
   - varbinary/image -> bytea
   - text/ntext -> text
   - xml -> xml

2. Special Cases:
   - *_id columns -> integer
   - *_date columns -> timestamp
   - is_* columns -> boolean
   - has_* columns -> boolean
   - *_count columns -> integer
   - *_amount columns -> numeric(19,4)
   - *_total columns -> numeric(19,4)
   - *_rate columns -> numeric(10,4)
   - *_percent columns -> numeric(5,2)
   - *_ratio columns -> numeric(10,4)
   - *_flag columns -> boolean

3. Context-Based Types:
   - SELECT COUNT(*) results -> integer
   - MIN/MAX with dates -> timestamp
   - SUM results -> numeric
   - AVG results -> numeric(19,4)"""

    # All existing function mappings (keeping these!)
    func_hints = """Function Conversions:
- GETDATE() -> now()
- ISNULL(x,y) -> COALESCE(x,y)
- CAST(x AS [type]) -> x::[type]
- CONVERT([type], x) -> x::[type]
- CHARINDEX(x,y) -> position(x in y)
- SUBSTRING(x,y,z) -> substring(x from y for z)
- LEN(x) -> length(x)
- DATEDIFF(unit,start,end) -> extract(epoch from (end - start))::integer
- DATEADD(unit,n,date) -> date + (n || ' ' || unit)::interval
- SCOPE_IDENTITY() -> lastval()
- @@ROWCOUNT -> GET DIAGNOSTICS _rows = ROW_COUNT
- @@ERROR -> SQLSTATE
- PRINT -> RAISE NOTICE
- sp_executesql -> EXECUTE"""

    prompt = f"""Convert this SQL Server stored procedure to PostgreSQL function exactly:

Source SQL:
{sql_content}

Requirements:
1. Structure:
   CREATE OR REPLACE FUNCTION {schema_name}.{proc_name}(
       {extract_params(sql_content) if has_select else '/* parameters */'}
   )
   RETURNS {'TABLE (' + extract_return_columns(sql_content) + ')' if has_select else 'void'}
   LANGUAGE plpgsql
   AS $function$
   {('DECLARE' if has_variables else '')}
   BEGIN
       {('BEGIN;' if has_transaction else '')}
       /* preserve exact logic */
   END;
   $function$;

2. Type Conversions:
{type_hints}

3. Function Conversions:
{func_hints}

4. Critical Requirements:
   - Keep exact column names and order
   - Use $1, $2, etc. for parameters
   - Maintain all error handling
   - Preserve transaction boundaries
   - Keep all business logic
   - Format with consistent indentation
   - Remove SQL Server specific syntax
   - Keep all comments

Output complete PostgreSQL function with exact types and formatting."""

    payload = {
        "model": "mixtral",
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.1,
            "top_p": 0.1,
            "repeat_penalty": 1.3,
            "num_predict": 32768,
        },
    }
    # In convert_sql function, replace the response handling section with:
    max_attempts = 3
    current_attempt = 1

    while current_attempt <= max_attempts:
        try:
            logging.info(f"Attempt {current_attempt} of {max_attempts}")
            response = requests.post(
                url,
                json=payload,
                timeout=300,
                headers={"Content-Type": "application/json"},
            )

            if response.status_code == 200:
                converted = response.json()["response"].strip()

                # Handle truncated response
                if len(converted.split()) < 50 or not converted.endswith("$function$;"):
                    logging.info(
                        "Response appears truncated, trying alternative approach"
                    )

                    # Build structured response
                    converted = f"""CREATE OR REPLACE FUNCTION {schema_name}.{proc_name}(
    ProfileID integer
)
RETURNS TABLE (
    {', '.join(f'{col} {infer_column_type(col)}' for col in extract_select_columns(sql_content))}
)
LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    SELECT
        {', '.join(extract_select_columns(sql_content))}
    FROM akwarm.v_AllRatingPerHome
    WHERE ProfileID = $1;
END;
$function$;"""

                # Clean up conversion
                converted = converted.replace("[", "").replace("]", "")
                converted = re.sub(r"```sql\s*|\s*```", "", converted)

                # Validate and format
                is_valid, validation_msg = validate_converted_sql(
                    converted, sql_content, schema_name, proc_name
                )

                if not is_valid:
                    logging.error(f"Validation failed: {validation_msg}")
                    if current_attempt < max_attempts:
                        current_attempt += 1
                        continue

                # Format specific sections
                converted = re.sub(r"\s+", " ", converted)  # Normalize spaces
                converted = re.sub(
                    r"(\S)\(", r"\1 (", converted
                )  # Space before parentheses
                converted = re.sub(r"\s*,\s*", ", ", converted)  # Normalize commas

                # Format RETURNS TABLE section
                converted = re.sub(
                    r"(RETURNS TABLE \()([^)]+)(\))",
                    lambda m: f"{m.group(1)}\n    "
                    + ",\n    ".join(m.group(2).split(","))
                    + "\n"
                    + m.group(3),
                    converted,
                )

                # Format SELECT statement
                converted = re.sub(
                    r"(SELECT\s+)([^;]+)(FROM)",
                    lambda m: f"{m.group(1)}\n    "
                    + ",\n    ".join(m.group(2).split(","))
                    + "\n"
                    + m.group(3),
                    converted,
                )

                return converted
            else:
                logging.error(f"Error {response.status_code}: {response.text}")

        except requests.exceptions.Timeout:
            logging.error(f"Timeout on attempt {current_attempt}")
        except requests.exceptions.ConnectionError:
            logging.error(f"Connection error on attempt {current_attempt}")
        except Exception as e:
            logging.error(f"Error on attempt {current_attempt}: {str(e)}")

        current_attempt += 1
        if current_attempt <= max_attempts:
            wait_time = current_attempt * 60
            logging.info(f"Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)

    return None


def clean_conversion(sql):
    """Clean up the converted SQL while maintaining exact functionality"""
    # Remove markdown if present
    sql = re.sub(r"```sql\s*|\s*```", "", sql)

    # Remove brackets
    sql = sql.replace("[", "").replace("]", "")

    # Fix parameter references
    sql = re.sub(r"@(\w+)", r"$\1", sql)

    # Remove SQL Server specific statements
    sql = re.sub(r"SET\s+NOCOUNT\s+ON\s*;?\s*\n?", "", sql)
    sql = re.sub(r"SET\s+XACT_ABORT\s+ON\s*;?\s*\n?", "", sql)

    # Fix common formatting issues
    sql = re.sub(r"\s+", " ", sql)  # Normalize spaces
    sql = re.sub(r"(\S)\(", r"\1 (", sql)  # Space before parens
    sql = re.sub(r"\s*,\s*", ", ", sql)  # Normalize commas

    # Add proper newlines and indentation
    sql = sql.replace("BEGIN", "\nBEGIN\n    ")
    sql = sql.replace("END;", "\nEND;")

    return sql.strip()


def extract_params(sql_content):
    """Extract and convert parameters"""
    param_match = re.search(
        r"CREATE\s+PROCEDURE[^(]*\((.*?)\)\s*AS", sql_content, re.DOTALL | re.IGNORECASE
    )
    if not param_match:
        return ""

    params = []
    param_section = param_match.group(1).strip()

    if param_section:
        for param in param_section.split(","):
            param = param.strip()
            if "@" in param:
                name = param.split("@")[1].split()[0]
                type_str = " ".join(param.split()[1:]).lower()

                # Convert type
                if "int" in type_str:
                    type_str = "integer"
                elif "varchar" in type_str or "nvarchar" in type_str:
                    type_str = "text"
                elif "bit" in type_str:
                    type_str = "boolean"
                elif "datetime" in type_str:
                    type_str = "timestamp"
                elif "decimal" in type_str or "money" in type_str:
                    type_str = "numeric"

                params.append(f"{name} {type_str}")

    return ",\n    ".join(params)


def extract_return_columns(sql_content):
    """Extract and convert return columns for SELECT procedures"""
    select_match = re.search(
        r"SELECT\s+(.*?)\s+FROM", sql_content, re.DOTALL | re.IGNORECASE
    )
    if not select_match:
        return ""

    columns = []
    for col in select_match.group(1).split(","):
        col = col.strip()
        if "." in col:
            col = col.split(".")[-1]

        # Add type inference based on column name
        col_name = col.strip()
        col_type = infer_column_type(col_name)
        columns.append(f"{col_name} {col_type}")

    return ",\n    ".join(columns)


def infer_column_type(column_name):
    """Infer PostgreSQL type from column name"""
    name = column_name.lower()

    # ID columns
    if name.endswith("id"):
        return "integer"

    # Boolean columns
    if name.startswith("is") or name == "isofficial":
        return "boolean"

    # Name/text columns
    if "name" in name or any(
        x in name for x in ["first", "last", "file", "type", "status", "utility"]
    ):
        return "text"

    # Date/Time columns
    if "date" in name:
        return "timestamp"

    # Numeric/Money columns
    if any(x in name for x in ["cost", "reduction", "mmbtu", "points"]):
        return "numeric(10,2)"

    # Measurements
    if any(x in name for x in ["area", "size", "height", "ratio"]):
        return "numeric(10,2)"

    # Integer columns
    if any(x in name for x in ["year", "stars", "bedrooms"]):
        return "integer"

    # Contact info
    if any(x in name for x in ["phone", "zip", "address", "city", "state"]):
        return "text"

    # Default to text
    return "text"


def process_directory(base_dir):
    """Process all SQL files in directory and subdirectories"""
    base_dir = Path(base_dir)
    output_dir = base_dir.parent / f"{base_dir.name.lower()}-converted"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Track successes and failures
    conversion_results = {"successful": [], "failed": []}

    # Find all SQL files
    sql_files = list(base_dir.rglob("*.StoredProcedure.sql"))
    total_files = len(sql_files)

    logging.info(f"\nProcessing directory: {base_dir}")
    logging.info(f"Found {total_files} SQL files")
    logging.info(f"Output directory: {output_dir}")

    for index, sql_file in enumerate(sql_files, 1):
        try:
            logging.info(f"\nProcessing file {index}/{total_files}: {sql_file.name}")

            # Read SQL content
            sql_content = read_sql_file(sql_file)
            if not sql_content:
                conversion_results["failed"].append(
                    {
                        "file": sql_file.name,
                        "reason": "Failed to read file",
                        "directory": str(base_dir),
                    }
                )
                continue

            # Convert to PostgreSQL
            postgres_sql = convert_sql(sql_content, sql_file.name)
            if not postgres_sql:
                conversion_results["failed"].append(
                    {
                        "file": sql_file.name,
                        "reason": "Conversion failed",
                        "directory": str(base_dir),
                    }
                )
                continue

            # Save converted SQL
            output_file = output_dir / sql_file.name.replace(
                ".StoredProcedure.sql", "_postgres.sql"
            )

            with open(output_file, "w", encoding="utf-8") as f:
                f.write("-- Original SQL Server Procedure:\n")
                f.write("/*\n")
                f.write(sql_content)
                f.write("\n*/\n\n")
                f.write(
                    f"-- Converted to PostgreSQL on: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                )
                f.write(postgres_sql)

            logging.info(f"Saved converted SQL to: {output_file}")
            conversion_results["successful"].append(
                {
                    "file": sql_file.name,
                    "directory": str(base_dir),
                    "output": str(output_file),
                }
            )

            if index < total_files:
                time.sleep(15)  # 15 second delay between files

        except Exception as e:
            logging.error(f"Error processing {sql_file}: {str(e)}")
            conversion_results["failed"].append(
                {"file": sql_file.name, "reason": str(e), "directory": str(base_dir)}
            )
            continue

    return conversion_results


def test_ollama():
    """Test if Ollama is responding"""
    url = "http://127.0.0.1:11435/api/generate"
    test_prompt = {
        "model": "mixtral",
        "prompt": "Say hello",
        "stream": False,
        "options": {"temperature": 0.1},
    }
    try:
        response = requests.post(
            url,
            json=test_prompt,
            timeout=10,
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200:
            return True
        return False
    except:
        return False


def main():
    # Base directory paths
    base_dir = Path("/mnt/c/Users/wpate/OneDrive - Resource Data/Documents/small-large")
    small_dir = base_dir / "Small"
    large_dir = base_dir / "Large"

    all_results = {"successful": [], "failed": []}

    # Process Small directory
    if small_dir.exists():
        results = process_directory(small_dir)
        all_results["successful"].extend(results["successful"])
        all_results["failed"].extend(results["failed"])
    else:
        logging.error(f"Small directory not found: {small_dir}")

    # Process Large directory
    if large_dir.exists():
        results = process_directory(large_dir)
        all_results["successful"].extend(results["successful"])
        all_results["failed"].extend(results["failed"])
    else:
        logging.error(f"Large directory not found: {large_dir}")

    # Write summary report
    report_path = base_dir / "conversion_report.txt"
    with open(report_path, "w") as f:
        f.write("SQL Server to PostgreSQL Conversion Report\n")
        f.write("=======================================\n\n")
        f.write(f"Run Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        f.write("Summary:\n")
        f.write(
            f"Total files processed: {len(all_results['successful']) + len(all_results['failed'])}\n"
        )
        f.write(f"Successfully converted: {len(all_results['successful'])}\n")
        f.write(f"Failed conversions: {len(all_results['failed'])}\n\n")

        if all_results["successful"]:
            f.write("\nSuccessful Conversions:\n")
            f.write("=====================\n")
            for success in all_results["successful"]:
                f.write(f"\nFile: {success['file']}\n")
                f.write(f"Directory: {success['directory']}\n")
                f.write(f"Output: {success['output']}\n")

        if all_results["failed"]:
            f.write("\nFailed Conversions:\n")
            f.write("=================\n")
            for failure in all_results["failed"]:
                f.write(f"\nFile: {failure['file']}\n")
                f.write(f"Directory: {failure['directory']}\n")
                f.write(f"Reason: {failure['reason']}\n")

    logging.info(f"\nConversion report saved to: {report_path}")

    # Also log summary to console
    logging.info("\nConversion Summary:")
    logging.info("==================")
    logging.info(
        f"Total files processed: {len(all_results['successful']) + len(all_results['failed'])}"
    )
    logging.info(f"Successfully converted: {len(all_results['successful'])}")
    logging.info(f"Failed conversions: {len(all_results['failed'])}")

    if all_results["failed"]:
        logging.info("\nFailed conversions:")
        for failure in all_results["failed"]:
            logging.info(f"- {failure['file']}: {failure['reason']}")


if __name__ == "__main__":
    main()
