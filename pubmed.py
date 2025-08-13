# -*- coding: utf-8 -*-
"""
This script connects to the NCBI server via HTTP and downloads all PubMed
baseline data files (.xml.gz) for a given year to a user-specified 
local directory.

It checks for existing files to avoid re-downloading and uses a thread 
pool for concurrent, faster downloads with a tqdm progress bar.
"""

import os
import sys
import requests
from tqdm import tqdm
import concurrent.futures

def download_file(url, local_filepath):
    """
    Downloads a single file from a URL to a local path.
    Designed to be run in a separate thread.

    Args:
        url (str): The URL of the file to download.
        local_filepath (str): The local path to save the file to.
    
    Returns:
        str: The path of the downloaded file if successful, otherwise None.
    """
    try:
        # Use streaming to handle large files without loading them all into memory
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

        total_size_in_bytes = int(response.headers.get('content-length', 0))
        block_size = 8192 # 8 Kibibytes for faster writing

        # Extract filename for the progress bar description
        filename = os.path.basename(local_filepath)

        with open(local_filepath, 'wb') as file, tqdm(
            desc=filename,
            total=total_size_in_bytes,
            unit='iB',
            unit_scale=True,
            unit_divisor=1024,
            leave=False # Hides the bar when done
        ) as progress_bar:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                file.write(data)
        
        # Check if the download was complete
        if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
            raise Exception("Incomplete download")
            
        return local_filepath

    except requests.exceptions.RequestException as e:
        # Clean up partial file on error
        if os.path.exists(local_filepath):
            os.remove(local_filepath)
        # We print the error in the main loop, so we just return None here
        return None


def download_all_pubmed_baseline_files(download_dir, year=2025, max_workers=10):
    """
    Downloads all PubMed baseline files for a given year from the NCBI server using HTTP.

    Args:
        download_dir (str): The full path to the local directory to save files in.
        year (int): The baseline year to download (e.g., 2025).
        max_workers (int): The maximum number of concurrent download threads.
    """
    # --- Step 1: Validate the download directory ---
    # Create the directory if it doesn't exist, which is more user-friendly.
    if not os.path.exists(download_dir):
        print(f"Directory '{download_dir}' not found. Creating it.")
        os.makedirs(download_dir)
    elif not os.path.isdir(download_dir):
        print(f"Error: The provided path '{download_dir}' exists but is not a directory.")
        sys.exit(1)

    # --- Step 2: Generate the list of potential URLs to download ---
    year_prefix = str(year)[-2:] 
    base_url = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
    num_files_to_try = 1250 
    all_possible_files = [f"pubmed{year_prefix}n{i:04d}.xml.gz" for i in range(1, num_files_to_try + 1)]
    
    # --- Step 3: Check for existing files and filter the download list ---
    print(f"Checking for existing files in '{os.path.abspath(download_dir)}'...")
    try:
        existing_files = set(os.listdir(download_dir))
        print(f"Found {len(existing_files)} files locally.")
    except FileNotFoundError:
        existing_files = set()

    files_to_download = [f for f in all_possible_files if f not in existing_files]
    skipped_count = len(all_possible_files) - len(files_to_download)

    if skipped_count > 0:
        print(f"Skipping {skipped_count} files that are already downloaded.")
    
    total_files = len(files_to_download)
    if total_files == 0:
        print("\nAll baseline files for this year appear to be downloaded. Nothing to do.")
        return
        
    print(f"\nPreparing to download {total_files} new files for the {year} baseline.")

    # --- Step 4: Confirmation before starting the large download ---
    confirm = input(f"This can be hundreds of gigabytes. Are you sure you want to continue? (y/n): ")
    if confirm.lower() != 'y':
        print("Download cancelled by user.")
        return

    # --- Step 5: Download files using a thread pool ---
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {
            executor.submit(download_file, base_url + filename, os.path.join(download_dir, filename)): filename
            for filename in files_to_download
        }

        for future in tqdm(concurrent.futures.as_completed(future_to_url), total=total_files, desc="Overall Progress", unit="file"):
            filename = future_to_url[future]
            try:
                result = future.result()
                if result is None:
                    pass
            except Exception as exc:
                tqdm.write(f'\n{filename} generated an exception: {exc}')


    print("\n\n--- Download process finished! ---")
    print(f"All files saved in: {os.path.abspath(download_dir)}")


# --- Main execution block ---
if __name__ == "__main__":
    download_path = input("Enter the full path to the directory where you want to save the PubMed files: ")

    download_all_pubmed_baseline_files(
        download_dir=download_path,
        year=2025,
        max_workers=10
    )

    print("\n--- Next Steps ---")
    print("1. Once downloaded, you will need to decompress the .gz files.")
    print("2. Finally, you will need to write a separate Python script to parse the XML content from each file.")
