import requests
import logging
from pathlib import Path
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def convert_sql(sql_content):
    """Simple conversion test"""
    url = "http://127.0.0.1:11435/api/generate"

    payload = {
        "model": "mixtral",
        "prompt": f"Convert this SQL Server code to PostgreSQL:\n\n{sql_content}",
        "stream": False,
    }

    try:
        response = requests.post(
            url, json=payload, timeout=30, headers={"Content-Type": "application/json"}
        )

        if response.status_code == 200:
            return response.json()["response"]
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

    # Read file
    with open(test_file, "r") as f:
        sql_content = f.read()

    # Try conversion
    postgres_sql = convert_sql(sql_content)

    if postgres_sql:
        print("\nConverted SQL:")
        print(postgres_sql)
    else:
        print("\nConversion failed!")


if __name__ == "__main__":
    main()
