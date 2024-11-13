from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Test path
test_dir = Path("/mnt/c/Users/wpate/OneDrive - Resource Data/Documents/small-large/small-converted")
test_file = test_dir / "test.txt"

try:
    # Create directory
    logging.debug(f"Attempting to create directory: {test_dir}")
    test_dir.mkdir(parents=True, exist_ok=True)
    logging.debug(f"Directory created/exists: {test_dir.exists()}")

    # Write test file
    logging.debug(f"Attempting to write file: {test_file}")
    with open(test_file, "w") as f:
        f.write("This is a test file")
    logging.debug(f"File created: {test_file.exists()}")

except Exception as e:
    logging.error(f"Failed with error: {str(e)}")