import os
import shutil
from math import ceil

def organize_files_into_batches(source_dir, target_dir, batch_size=5):
    print(f"Looking for files in: {source_dir}")

    # Create target directory if it doesn't exist
    if not os.path.exists(target_dir):
        print(f"Creating target directory: {target_dir}")
        os.makedirs(target_dir)

    # Get all .StoredProcedure.sql files
    files = [f for f in os.listdir(source_dir) if f.endswith('.StoredProcedure.sql')]
    print(f"Found {len(files)} .StoredProcedure.sql files")

    if not files:
        print("No .StoredProcedure.sql files found! Please check the source directory path.")
        return

    # Calculate number of batches needed
    num_batches = ceil(len(files) / batch_size)
    print(f"Will create {num_batches} batches")

    # Create batches and move files
    for batch_num in range(num_batches):
        # Create batch directory
        batch_dir = os.path.join(target_dir, f'batch{batch_num + 1}')
        if not os.path.exists(batch_dir):
            os.makedirs(batch_dir)

        # Get files for this batch
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, len(files))
        batch_files = files[start_idx:end_idx]

        # Copy files to batch directory
        for file in batch_files:
            source_file = os.path.join(source_dir, file)
            target_file = os.path.join(batch_dir, file)
            print(f"Copying {file} to batch{batch_num + 1}")
            shutil.copy2(source_file, target_file)

        print(f'Created batch{batch_num + 1} with {len(batch_files)} files')

# Paths using WSL mount points
source_directory = "/mnt/c/Users/wpate/OneDrive - Resource Data/Documents/alaska"
target_directory = "/mnt/c/Users/wpate/OneDrive - Resource Data/Documents/alaska-batches"

print("Starting file organization...")
print(f"Source directory exists: {os.path.exists(source_directory)}")
print(f"Current working directory: {os.getcwd()}")

# Run the organization
organize_files_into_batches(source_directory, target_directory)