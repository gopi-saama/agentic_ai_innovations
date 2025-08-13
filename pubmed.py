# -*- coding: utf-8 -*-
"""
This script connects to the NCBI server via HTTP and downloads all PubMed
baseline data files (.xml.gz) to a user-specified local directory.
It uses the requests library for more reliable downloads and tqdm for a progress bar.
"""

import os
import sys
import requests
from tqdm import tqdm

def download_all_pubmed_baseline_files(download_dir):
    """
    Downloads all PubMed baseline files from the NCBI server using HTTP.

    Args:
        download_dir (str): The full path to the local directory to save files in.
    """
    # --- Step 1: Validate the download directory ---
    if not os.path.isdir(download_dir):
        print(f"Error: The provided path '{download_dir}' is not a valid directory.")
        print("Please create the directory first or provide a valid path.")
        sys.exit(1) # Exit the script if the path is invalid

    # --- Step 2: Generate the list of URLs to download ---
    # This pattern covers all 1219 baseline files for 2024
    base_url = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
    files_to_download = [f"pubmed25n{i:04d}.xml.gz" for i in range(1, 1220)]
    
    total_files = len(files_to_download)
    print(f"Found {total_files} '.xml.gz' files to download.")

    # --- Step 3: Confirmation before starting the large download ---
    confirm = input(f"This will download {total_files} files, which can be hundreds of gigabytes. Are you sure you want to continue? (y/n): ")
    if confirm.lower() != 'y':
        print("Download cancelled by user.")
        return

    # --- Step 4: Download each file with a progress bar ---
    # tqdm wraps the loop to show overall progress
    for filename in tqdm(files_to_download, desc="Overall Progress", unit="file"):
        url = base_url + filename
        local_filepath = os.path.join(download_dir, filename)

        try:
            # Use streaming to handle large files without loading them all into memory
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

            total_size_in_bytes = int(response.headers.get('content-length', 0))
            block_size = 1024 # 1 Kibibyte

            # Nested tqdm for individual file download progress
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
                print(f"ERROR, something went wrong with {filename}")
                # Optional: remove partial file
                # os.remove(local_filepath)

        except requests.exceptions.RequestException as e:
            print(f"\nError downloading {filename}: {e}. Skipping file.")
            if os.path.exists(local_filepath):
                os.remove(local_filepath) # Clean up partial file

    print("\n\n--- Download process finished! ---")
    print(f"All files saved in: {os.path.abspath(download_dir)}")


# --- Main execution block ---
if __name__ == "__main__":
    # --- Prompt user for the download path ---
    download_path = input("Enter the full path to the directory where you want to save the PubMed files: ")

    # --- Start the download process ---
    download_all_pubmed_baseline_files(download_path)

    print("\n--- Next Steps ---")
    print("1. Once downloaded, you will need to decompress the .gz files.")
    print("2. Finally, you will need to write a separate Python script to parse the XML content from each file to extract the abstracts and other data.")
    print("   (Consider using libraries like 'xml.etree.ElementTree' or 'lxml' for parsing).")
