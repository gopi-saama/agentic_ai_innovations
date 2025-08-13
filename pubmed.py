# -*- coding: utf-8 -*-
"""
This script connects to the NCBI server via HTTP and downloads all PubMed
baseline data files (.xml.gz) for a given year to a user-specified 
local directory.

It performs MD5 checksum validation to ensure file integrity, checks for 
existing files to avoid re-downloading, and uses a thread pool for 
concurrent, faster downloads with a tqdm progress bar.
"""

import os
import sys
import requests
from tqdm import tqdm
import concurrent.futures
import hashlib

def calculate_md5(filepath, block_size=65536):
    """Calculates the MD5 hash of a file."""
    md5 = hashlib.md5()
    try:
        with open(filepath, 'rb') as f:
            while True:
                data = f.read(block_size)
                if not data:
                    break
                md5.update(data)
    except IOError:
        return None
    return md5.hexdigest()

def verify_and_download_file(filename, download_dir, base_url):
    """
    Manages the download and verification for a single file.
    Checks existing files and verifies new downloads against MD5 checksums.
    
    Returns:
        str: Status ('success', 'skipped_verified', 'failed').
        str: The filename processed.
    """
    local_filepath = os.path.join(download_dir, filename)
    url = base_url + filename
    md5_url = url + ".md5"

    try:
        # --- Get Official MD5 Checksum ---
        md5_response = requests.get(md5_url, timeout=30)
        md5_response.raise_for_status()
        official_hash = md5_response.text.split('=')[1].strip()

        # --- Check if file exists and is valid ---
        if os.path.exists(local_filepath):
            local_hash = calculate_md5(local_filepath)
            if local_hash == official_hash:
                return 'skipped_verified', filename
            else:
                tqdm.write(f"MD5 mismatch for existing file {filename}. Deleting and redownloading.")
                os.remove(local_filepath)

        # --- Download the file ---
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        total_size = int(response.headers.get('content-length', 0))
        
        with open(local_filepath, 'wb') as file:
            for data in response.iter_content(chunk_size=8192):
                file.write(data)
        
        # --- Verify the newly downloaded file ---
        if os.path.getsize(local_filepath) != total_size:
             raise Exception("Incomplete download (size mismatch)")

        new_local_hash = calculate_md5(local_filepath)
        if new_local_hash == official_hash:
            return 'success', filename
        else:
            raise Exception(f"MD5 mismatch after download. Expected {official_hash}, got {new_local_hash}")

    except Exception as e:
        tqdm.write(f"Error processing {filename}: {e}")
        if os.path.exists(local_filepath):
            os.remove(local_filepath) # Clean up partial/corrupt file
        return 'failed', filename


def download_all_pubmed_baseline_files(download_dir, year=2025, max_workers=10, max_retries=3):
    """
    Downloads all PubMed baseline files for a given year from the NCBI server using HTTP.

    Args:
        download_dir (str): The full path to the local directory to save files in.
        year (int): The baseline year to download (e.g., 2025).
        max_workers (int): The maximum number of concurrent download threads.
        max_retries (int): The number of times to retry downloading failed files.
    """
    if not os.path.exists(download_dir):
        print(f"Directory '{download_dir}' not found. Creating it.")
        os.makedirs(download_dir)
    elif not os.path.isdir(download_dir):
        print(f"Error: The provided path '{download_dir}' exists but is not a directory.")
        sys.exit(1)

    year_prefix = str(year)[-2:] 
    base_url = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
    num_files_to_try = 1250 
    files_to_process = [f"pubmed{year_prefix}n{i:04d}.xml.gz" for i in range(1, num_files_to_try + 1)]
    
    for attempt in range(max_retries):
        if not files_to_process:
            print("All files have been successfully downloaded and verified.")
            break

        print(f"\n--- Attempt {attempt + 1} of {max_retries} ---")
        print(f"Processing {len(files_to_process)} files...")
        
        failed_files = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_filename = {
                executor.submit(verify_and_download_file, filename, download_dir, base_url): filename
                for filename in files_to_process
            }

            for future in tqdm(concurrent.futures.as_completed(future_to_filename), total=len(files_to_process), desc="Overall Progress"):
                status, filename = future.result()
                if status == 'failed':
                    failed_files.append(filename)
        
        files_to_process = sorted(failed_files)

    if files_to_process:
        print("\n--- Download process finished with errors ---")
        print("The following files could not be downloaded or verified:")
        for f in files_to_process:
            print(f"- {f}")
    else:
        print("\n\n--- Download process finished successfully! ---")
        print(f"All files saved and verified in: {os.path.abspath(download_dir)}")


# --- Main execution block ---
if __name__ == "__main__":
    download_path = input("Enter the full path to the directory where you want to save the PubMed files: ")

    download_all_pubmed_baseline_files(
        download_dir=download_path,
        year=2025,
        max_workers=10,
        max_retries=3
    )

    print("\n--- Next Steps ---")
    print("1. Once downloaded, you will need to decompress the .gz files.")
    print("2. Finally, you will need to write a separate Python script to parse the XML content from each file.")
