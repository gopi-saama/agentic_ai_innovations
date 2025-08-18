# -*- coding: utf-8 -*-
"""
This script connects to the NCBI server via HTTP and downloads all PubMed
baseline data files (.xml.gz) for a given year to a user-specified 
local directory.

It dynamically determines the number of files to download by querying the
FTP server first. It then performs MD5 checksum validation, checks for 
existing files, retries failed downloads, and runs a final check to ensure 
no files are missing from the sequence.
"""

import os
import sys
import requests
from tqdm import tqdm
import concurrent.futures
import hashlib
import argparse

from list_pubmed_files import count_remote_files

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
    
    Returns:
        str: Status ('success', 'skipped_verified', 'failed_404', 'failed').
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

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return 'failed_404', filename # File doesn't exist on server
        tqdm.write(f"HTTP Error processing {filename}: {e}")
        if os.path.exists(local_filepath): os.remove(local_filepath)
        return 'failed', filename
    except Exception as e:
        tqdm.write(f"Error processing {filename}: {e}")
        if os.path.exists(local_filepath): os.remove(local_filepath)
        return 'failed', filename


def download_all_pubmed_baseline_files(download_dir, year, max_workers, max_retries):
    """
    Downloads all PubMed baseline files for a given year from the NCBI server using HTTP.
    """
    if not os.path.exists(download_dir):
        print(f"Directory '{download_dir}' not found. Creating it.")
        os.makedirs(download_dir)
    elif not os.path.isdir(download_dir):
        print(f"Error: The provided path '{download_dir}' exists but is not a directory.")
        sys.exit(1)

    # --- Dynamically determine the number of files to download ---
    num_files_to_try = count_remote_files(year)
    if not num_files_to_try:
        print(f"Could not determine the number of files for year {year}, or no files found. Exiting.")
        sys.exit(1)

    year_prefix = str(year)[-2:] 
    base_url = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
    files_to_process = [f"pubmed{year_prefix}n{i:04d}.xml.gz" for i in range(1, num_files_to_try + 1)]
    
    non_existent_files = set()

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
                elif status == 'failed_404':
                    non_existent_files.add(filename)
        
        files_to_process = sorted(failed_files)

    # --- Final integrity check for missing files ---
    print("\n--- Performing final check for missing files in sequence ---")
    
    all_possible_files = {f"pubmed{year_prefix}n{i:04d}.xml.gz" for i in range(1, num_files_to_try + 1)}
    expected_files = sorted(list(all_possible_files - non_existent_files))
    
    downloaded_files = set(os.listdir(download_dir))
    
    missing_files = [f for f in expected_files if f not in downloaded_files]

    if files_to_process or missing_files:
        print("\n--- Download process finished with errors ---")
        if files_to_process:
            print("\nThe following files could not be downloaded or verified after all retries:")
            for f in files_to_process:
                print(f"- {f}")
        if missing_files:
            print("\nThe following files are missing from the download directory:")
            for f in missing_files:
                print(f"- {f}")
    else:
        print(f"\n\n--- Download process finished successfully! ---")
        print(f"All {len(expected_files)} files saved and verified in: {os.path.abspath(download_dir)}")


# --- Main execution block ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download and verify PubMed baseline files concurrently.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument("download_path", help="Full path to the directory to save PubMed files.")
    
    parser.add_argument("--year", type=int, default=2025, help="The baseline year to download.")
    
    parser.add_argument(
        "--max_workers", 
        type=int, 
        default=10, 
        help="Maximum number of concurrent download threads."
    )
    
    parser.add_argument(
        "--max_retries", 
        type=int, 
        default=3, 
        help="Number of times to retry downloading failed files."
    )

    args = parser.parse_args()

    download_all_pubmed_baseline_files(
        download_dir=args.download_path,
        year=args.year,
        max_workers=args.max_workers,
        max_retries=args.max_retries
    )
