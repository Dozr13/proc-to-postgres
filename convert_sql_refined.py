import os
import time
import requests
import json
import logging
import re
from pathlib import Path
from collections import defaultdict

# Constants
OLLAMA_URL = "http://localhost:11435/api/generate"
RETRY_MAX = 3

conversion_failures = defaultdict(list)


def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("conversion.log"), logging.StreamHandler()],
    )


def check_ollama_connection():
    try:
        response = requests.get("http://localhost:11435/api/tags", timeout=5)
        return response.status_code == 200
    except requests.RequestException:
        logging.error(
            "Could not connect to Ollama server. Ensure Docker container is running."
        )
        return False


def read_sql_file(file_path):
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


def convert_chunk(prompt, model="mixtral"):
    """Convert a single chunk of SQL"""
    try:
        response = requests.post(
            OLLAMA_URL,
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.1,
                    "num_predict": 32768,
                    "num_ctx": 32768,
                    "top_p": 0.1,
                },
            },
            timeout=(30, 300),
        )

        if response.status_code == 200:
            result = response.json().get("response", "").strip()
            if "CREATE OR REPLACE FUNCTION" in result or "BEGIN" in result:
                return result

    except Exception as e:
        logging.error(f"Error converting chunk: {str(e)}")
    return None


def fix_cte_formatting(sql):
    """Fix specific CTE and formatting issues"""
    lines = []
    current_cte = None
    in_cte = False
    indent_level = 0

    for line in sql.splitlines():
        stripped = line.strip()

        # Handle WITH start
        if stripped.startswith("WITH "):
            in_cte = True
            indent_level = 1
            lines.append(stripped)
            continue

        # Handle CTE definitions
        if in_cte and " AS (" in stripped:
            current_cte = stripped.split(" AS (")[0].strip()
            lines.append(f"    {current_cte} AS (")
            indent_level = 2
            continue

        # Handle CTE ends
        if stripped == "),":
            indent_level = 1
            lines.append("    ),")
            continue

        # Handle SELECT statements in CTEs
        if in_cte and stripped.startswith("SELECT"):
            lines.append(f"{'    ' * indent_level}SELECT")
            continue

        # Handle FROM statements
        if stripped.startswith("FROM "):
            if not stripped.endswith("FROM"):  # Only if table name exists
                lines.append(f"{'    ' * indent_level}FROM {stripped[5:]}")
            continue

        # Handle JOIN statements
        if any(
            stripped.startswith(join)
            for join in ["LEFT JOIN", "INNER JOIN", "RIGHT JOIN"]
        ):
            lines.append(f"{'    ' * indent_level}{stripped}")
            continue

        # Handle WHERE clauses
        if stripped.startswith("WHERE "):
            lines.append(f"{'    ' * indent_level}{stripped}")
            continue

        # Handle normal lines within CTEs
        if in_cte:
            lines.append(f"{'    ' * indent_level}{stripped}")
            continue

        # Handle everything else
        lines.append(stripped)

        # Check for end of CTEs
        if in_cte and stripped.endswith(")") and not stripped.endswith("AS ("):
            in_cte = False
            indent_level = 0

    return "\n".join(lines)


def handle_large_file(sql_content, schema_name, proc_name):
    try:
        # Initial cleanup
        sql_clean = sql_content.replace("SET NOCOUNT ON", "")
        sql_clean = re.sub(r"/\*.*?\*/", "", sql_clean, flags=re.DOTALL)
        sql_clean = re.sub(r"--.*$", "", sql_clean, flags=re.MULTILINE)
        sql_clean = re.sub(r"\[(\w+)\]", r"\1", sql_clean)
        sql_clean = re.sub(r"\bGO\b", "", sql_clean)

        # Parse and build the function header
        header_pattern = r"CREATE\s+PROCEDURE.*?\((.*?)\)\s*AS"
        header_match = re.search(header_pattern, sql_clean, re.DOTALL | re.IGNORECASE)
        if not header_match:
            return None

        # Build the main structure
        output_parts = []
        output_parts.append(f"CREATE OR REPLACE FUNCTION {schema_name}.{proc_name}(")

        # Handle parameters
        params_text = header_match.group(1)
        params = []
        for param in [p.strip() for p in params_text.split(",") if p.strip()]:
            param_match = re.match(r"@(\w+)\s+([^=\s]+)(?:\s*=\s*([^,\s]+))?", param)
            if param_match:
                name, type_str, default = param_match.groups()
                pg_type = {
                    "varchar": "text",
                    "nvarchar": "text",
                    "bit": "boolean",
                    "datetime": "timestamp",
                    "money": "numeric",
                    "int": "integer",
                }.get(type_str.lower().split("(")[0], type_str.lower())

                param_str = f"    _{name.lower()} {pg_type}"
                if default:
                    param_str += f" DEFAULT {default.replace('NULL', 'null')}"
                params.append(param_str)

        output_parts.append(",\n".join(params))
        output_parts.extend(
            [
                ")",
                "    RETURNS void",
                "    LANGUAGE plpgsql",
                "    STABLE",
                "AS $$",
                "DECLARE",
            ]
        )

        # Handle declarations
        declarations = set()
        declarations.add("    _modified_by text;")
        declare_matches = re.finditer(
            r"DECLARE\s+@(\w+)\s+([^;\n]+)", sql_clean, re.IGNORECASE
        )
        for match in declare_matches:
            name, type_str = match.groups()
            pg_type = {
                "varchar": "text",
                "nvarchar": "text",
                "bit": "boolean",
                "int": "integer",
            }.get(type_str.lower().split("(")[0], type_str.lower())
            declarations.add(f"    _{name.lower()} {pg_type};")

        output_parts.extend(sorted(declarations))
        output_parts.append("BEGIN")

        # Extract CTEs and main body
        cte_pattern = r"WITH\s+(.*?)\s+SELECT"
        cte_match = re.search(cte_pattern, sql_clean, re.DOTALL | re.IGNORECASE)

        if cte_match:
            ctes_text = cte_match.group(1)
            # Split CTEs
            cte_definitions = re.split(r",\s*(\w+)\s+AS\s*\(", ctes_text)
            formatted_ctes = ["WITH"]

            for i in range(1, len(cte_definitions), 2):
                cte_name = cte_definitions[i]
                cte_content = cte_definitions[i + 1].strip()
                if cte_content.endswith(")"):
                    cte_content = cte_content[:-1]

                formatted_ctes.extend([f"{cte_name} AS (", f"    {cte_content}", "),"])

            # Remove last comma
            if formatted_ctes[-1].endswith(","):
                formatted_ctes[-1] = formatted_ctes[-1][:-1]

            output_parts.extend(formatted_ctes)

        # Convert the rest of the body
        replacements = {
            r"@(\w+)": r"_\1",
            r"\[dbo\]\.": "",
            r"ISNULL\((.*?),(.*?)\)": r"COALESCE(\1,\2)",
            r"GETDATE\(\)": "CURRENT_TIMESTAMP",
            r"SCOPE_IDENTITY\(\)": "lastval()",
            r"DATEDIFF\(day,(.*?),(.*?)\)": r"\1 - \2",
            r"LEN\((.*?)\)": r"length(\1)",
            r"SET\s+@(\w+)\s*=": r"_\1 :=",
            r"SELECT\s+TOP\s+1": "SELECT",
            r"akrebate\.dbo\.": "akrebate.",
        }

        body = re.search(r"BEGIN\s+(.*?)END\s*$", sql_clean, re.DOTALL | re.IGNORECASE)
        if body:
            converted_body = body.group(1)
            for pattern, repl in replacements.items():
                converted_body = re.sub(
                    pattern, repl, converted_body, flags=re.IGNORECASE
                )
            output_parts.append(converted_body)

        output_parts.extend(["END;", "$$;"])

        return "\n".join(output_parts)

    except Exception as e:
        logging.error(f"Error processing large file {proc_name}: {str(e)}")
        return None


def convert_to_postgres(sql_content, schema_name, proc_name):
    content_size = len(sql_content)
    logging.info(f"Converting {proc_name} (size: {content_size} chars)")

    # Always use chunked processing for large files
    if content_size > 4000:  # Lower threshold to catch more complex procedures
        logging.warning(f"Large file detected ({content_size} chars): {proc_name}")
        return handle_large_file(sql_content, schema_name, proc_name)

    # Choose model based on content size
    selected_model = "mixtral" if content_size > 30000 else "codellama"
    logging.info(f"Using model: {selected_model} for {proc_name}")

    prompt = f"""Convert this SQL Server procedure to PostgreSQL.
You must include 'CREATE OR REPLACE FUNCTION' in your response.
Use this exact format:

CREATE OR REPLACE FUNCTION schema.name(parameters)
    RETURNS [return_type]
    STABLE
    AS $$
    BEGIN
        -- converted code here
    END;
    $$ LANGUAGE plpgsql;

Schema: {schema_name}
Procedure: {proc_name}

SQL to convert:
{sql_content}"""

    attempt = 0
    while attempt < RETRY_MAX:
        try:
            logging.info(f"Sending request to Ollama (attempt {attempt + 1})")
            start_time = time.time()

            # Health check
            try:
                test_response = requests.get(
                    "http://localhost:11435/api/tags", timeout=5
                )
                if test_response.status_code != 200:
                    logging.error("Ollama server not responding properly")
                    raise requests.RequestException("Ollama server not healthy")
            except requests.RequestException as e:
                logging.error(f"Ollama health check failed: {str(e)}")
                raise

            response = requests.post(
                OLLAMA_URL,
                json={
                    "model": selected_model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.1,
                        "num_predict": 4096,
                        "num_ctx": 4096,
                        "top_p": 0.1,
                    },
                },
                timeout=(10, 120),
            )

            elapsed_time = time.time() - start_time
            logging.info(f"Ollama response received after {elapsed_time:.2f} seconds")

            if response.status_code == 200:
                result_json = response.json()
                result = result_json.get("response", "").strip()

                # Log the actual response for debugging
                logging.info(f"Raw response: {result[:200]}...")

                if result and "CREATE OR REPLACE FUNCTION" in result:
                    logging.info(f"Valid SQL response received for {proc_name}")
                    sql = extract_sql_body(result)
                    sql = replace_positional_params(sql, sql_content)
                    return format_final_sql(sql)
                else:
                    logging.warning(
                        f"Attempt {attempt + 1}: No valid SQL in response for {proc_name}"
                    )
                    logging.warning(
                        "Response did not contain expected 'CREATE OR REPLACE FUNCTION'"
                    )
            else:
                logging.error(f"Unexpected status code: {response.status_code}")

            attempt += 1
            if attempt < RETRY_MAX:
                wait_time = min(2**attempt * 5, 60)
                logging.info(f"Waiting {wait_time} seconds before retry {attempt + 1}")
                time.sleep(wait_time)

        except requests.RequestException as e:
            logging.error(f"Request error during conversion of {proc_name}: {str(e)}")
            attempt += 1
            if attempt < RETRY_MAX:
                wait_time = min(2**attempt * 5, 60)
                logging.info(f"Waiting {wait_time} seconds before retry after error")
                time.sleep(wait_time)

    logging.error(f"Failed to convert {proc_name} after {RETRY_MAX} attempts.")
    return None


def extract_sql_body(response_text):
    start = response_text.find("CREATE OR REPLACE FUNCTION")
    end = response_text.rfind(";") + 1
    return response_text[start:end]


def replace_positional_params(sql, original_content):
    params = extract_parameters(original_content)
    param_map = {param[0]: f"${i+1}" for i, param in enumerate(params)}
    for param_name, pos_placeholder in param_map.items():
        sql = re.sub(rf"\b{param_name}\b", pos_placeholder, sql)
    return sql


def format_final_sql(sql):
    """Format SQL with clean, consistent spacing and indentation"""
    lines = []
    in_cte = False
    in_select = False
    cte_level = 0

    # Split into lines and process each one
    for line in sql.splitlines():
        stripped = line.strip()

        # Skip empty lines
        if not stripped:
            continue

        # Handle CREATE FUNCTION
        if "CREATE OR REPLACE FUNCTION" in stripped:
            lines.append(stripped)
            continue

        # Handle parameters
        if stripped.startswith("_") and ")" not in stripped:
            lines.append(f"    {stripped}")
            continue

        # Handle WITH clauses (CTEs)
        if stripped.upper().startswith("WITH "):
            lines.append("\n" + stripped)
            in_cte = True
            cte_level = 1
            continue

        # Handle CTE definitions
        if in_cte and stripped.endswith(" AS ("):
            lines.append(f"{'    ' * cte_level}{stripped}")
            cte_level += 1
            continue

        # Handle end of CTE
        if in_cte and stripped == "),":
            cte_level -= 1
            lines.append(f"{'    ' * cte_level}{stripped}")
            continue

        # Handle SELECT statements
        if stripped.upper().startswith("SELECT"):
            current_indent = "    " * cte_level
            lines.append(f"{current_indent}SELECT")
            in_select = True
            continue

        # Handle FROM clauses
        if stripped.upper().startswith("FROM"):
            current_indent = "    " * cte_level
            lines.append(f"{current_indent}FROM")
            continue

        # Handle JOIN clauses
        if any(
            stripped.upper().startswith(join)
            for join in ["LEFT JOIN", "INNER JOIN", "RIGHT JOIN"]
        ):
            current_indent = "    " * cte_level
            lines.append(f"{current_indent}{stripped}")
            continue

        # Handle WHERE clauses
        if stripped.upper().startswith("WHERE"):
            current_indent = "    " * cte_level
            lines.append(f"{current_indent}WHERE {stripped[6:]}")
            continue

        # Handle CASE statements
        if stripped.upper().startswith("CASE"):
            current_indent = "    " * (cte_level + 1)
            lines.append(f"{current_indent}{stripped}")
            continue

        # Handle END for CASE statements
        if stripped.upper() == "END":
            current_indent = "    " * cte_level
            lines.append(f"{current_indent}{stripped}")
            continue

        # Handle select columns
        if in_select and not any(
            clause in stripped.upper()
            for clause in ["FROM", "WHERE", "GROUP BY", "ORDER BY"]
        ):
            current_indent = "    " * (cte_level + 1)
            lines.append(f"{current_indent}{stripped}")
            continue

        # Handle standard statements
        current_indent = "    " * cte_level
        lines.append(f"{current_indent}{stripped}")

        # Reset flags based on content
        if stripped.endswith(";"):
            in_select = False
            if in_cte:
                cte_level -= 1
            if cte_level == 0:
                in_cte = False

    # Join all lines
    formatted_sql = "\n".join(lines)

    # Final cleanup
    formatted_sql = re.sub(
        r"\s+;", ";", formatted_sql
    )  # Clean up spaces before semicolons
    formatted_sql = re.sub(
        r"\(\s+\)", "()", formatted_sql
    )  # Clean up empty parentheses

    return formatted_sql


def clean_sql_content(sql):
    """Clean up SQL content before processing"""
    # Remove GO statements
    sql = re.sub(r"\bGO\b", "", sql)

    # Fix common spacing issues
    sql = re.sub(r"\s+", " ", sql)

    # Fix CASE WHEN spacing
    sql = re.sub(r"CASE\s+WHEN", "CASE WHEN", sql, flags=re.IGNORECASE)

    # Fix END CASE spacing
    sql = re.sub(r"END\s+CASE", "END CASE", sql, flags=re.IGNORECASE)

    # Fix parentheses spacing
    sql = re.sub(r"\(\s+", "(", sql)
    sql = re.sub(r"\s+\)", ")", sql)

    # Fix common function calls
    sql = re.sub(r"ISNULL\s*\(", "COALESCE(", sql, flags=re.IGNORECASE)
    sql = re.sub(r"GETDATE\s*\(\)", "CURRENT_TIMESTAMP", sql, flags=re.IGNORECASE)

    return sql.strip()


def extract_schema_proc_name(filename):
    parts = filename.replace(".StoredProcedure.sql", "").split(".")
    return parts[0].lower(), parts[1].lower().replace(".", "_")


def save_converted_sql(output_dir, original_name, converted_sql):
    output_file = (
        output_dir / f"{original_name.replace('.StoredProcedure.sql', '_postgres.sql')}"
    )
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(converted_sql.strip() + "\n")


def main():
    setup_logging()
    if not check_ollama_connection():
        logging.error("Ollama server is not accessible.")
        return

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

                schema_name, proc_name = extract_schema_proc_name(sql_file.name)
                converted_sql = convert_to_postgres(sql_content, schema_name, proc_name)

                if converted_sql:
                    save_converted_sql(output_dir, sql_file.name, converted_sql)
                    logging.info(f"Converted: {sql_file.name}")
                time.sleep(0.5)

            except Exception as e:
                logging.error(f"Failed to process {sql_file}: {e}")
                continue


if __name__ == "__main__":
    main()
