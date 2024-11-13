import os
from pathlib import Path
import logging
from convert_sql_adjustments import convert_to_postgres, read_sql_file, setup_logging


def test_single_file():
    # Setup logging
    setup_logging()

    # Test file path
    file_path = Path(
        "/mnt/c/Users/wpate/OneDrive - Resource Data/Documents/small-large/Small/batch1/akwarm.GetAllRatingsByProfileID.StoredProcedure.sql"
    )

    # Create output directory
    output_dir = Path(
        "/mnt/c/Users/wpate/OneDrive - Resource Data/Documents/small-large/Small/test_output"
    )
    output_dir.mkdir(exist_ok=True)

    # Read and convert
    sql_content = read_sql_file(file_path)
    if sql_content:
        print("\nOriginal SQL:")
        print("-" * 80)
        print(sql_content)
        print("-" * 80)

        postgres_sql = convert_to_postgres(sql_content, file_path.name)

        if postgres_sql:
            print("\nConverted PostgreSQL:")
            print("-" * 80)
            print(postgres_sql)
            print("-" * 80)

            # Save the result
            output_path = output_dir / f"{file_path.stem}_converted.sql"
            with open(output_path, "w") as f:
                f.write(postgres_sql)
            print(f"\nSaved converted SQL to: {output_path}")
        else:
            print("Conversion failed!")
    else:
        print("Failed to read SQL file!")


if __name__ == "__main__":
    test_single_file()
