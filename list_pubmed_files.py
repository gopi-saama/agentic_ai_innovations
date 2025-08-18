# -*- coding: utf-8 -*-
"""
This utility script connects to the NCBI FTP server and counts the 
baseline files for a given year.
"""

import ftplib

def count_remote_files(year):
    """
    Connects to the NCBI FTP server and counts the .xml.gz files for a given year.

    Args:
        year (int): The baseline year to count files for.
    
    Returns:
        int: The number of files found, or None if an error occurs.
    """
    FTP_HOST = "ftp.ncbi.nlm.nih.gov"
    FTP_DIR = "/pubmed/baseline/"
    
    year_prefix = f"pubmed{str(year)[-2:]}n"

    print(f"Connecting to {FTP_HOST} to count remote files for year {year}...")

    try:
        with ftplib.FTP(FTP_HOST, timeout=30) as ftp:
            ftp.login()  # Anonymous login
            ftp.cwd(FTP_DIR)

            # Get a simple list of filenames
            all_files = ftp.nlst()
            
            file_count = 0
            for filename in all_files:
                if filename.startswith(year_prefix) and filename.endswith('.xml.gz'):
                    file_count += 1
            
            print(f"Found {file_count} remote files for year {year}.")
            return file_count

    except ftplib.all_errors as e:
        print(f"\nAn FTP error occurred while counting files: {e}")
        return None
    except Exception as e:
        print(f"\nAn unexpected error occurred while counting files: {e}")
        return None

# This part is for direct testing of this script.
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Count remote PubMed baseline files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("year", type=int, help="The baseline year to count files for (e.g., 2024, 2025).")
    args = parser.parse_args()
    count_remote_files(args.year)
