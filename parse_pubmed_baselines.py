#!/usr/bin/env python3
import os
import sys
import json
import gzip
from tqdm import tqdm
from pathlib import Path
import pubmed_parser as pp
import concurrent.futures
import threading

def parse_pubmed_file(file_path):
    """
    Parse a MEDLINE XML file using pubmed_parser
    
    Args:
        file_path (str): Path to the XML file (can be gzipped)
        
    Returns:
        tuple: (article_data, file_path)
    """
    
    # Parse article data
    article_data = list(pp.parse_medline_xml(
        file_path,
        year_info_only=False,  # Extract month and day information
        nlm_category=True,     # Parse structured abstract
        author_list=True,      # Return author list as a list
        reference_list=True    # Return reference list as a list
    ))
    
    # Extract grant data from article data
    # (parse_medline_xml already includes grant_ids in each article)
    for article in article_data:
        if 'grant_ids' in article and article['grant_ids']:
            pmid = article.get('pmid', '')
            for grant in article['grant_ids']:
                # Add PMID to each grant record for reference
                grant['pmid'] = pmid
    
    return article_data, file_path

def process_publication_data(raw_data: dict, remove_title_brackets: bool = False) -> dict:
    """
    Parses, cleans, and restructures a JSON object representing publication data.

    This function performs several cleanup and merging operations:
    1. Combines 'forename' and 'lastname' for each author into a single full name.
    2. Splits semicolon-separated strings (for mesh terms, publication types,
       chemical list, and keywords) into lists.
    3. Keeps other fields as is.
    4. Creates a new dictionary with the 'pmid' as the main key.
    5. Optionally removes square brackets from title text.

    Args:
        raw_data (dict): The original JSON object.
        remove_title_brackets (bool): If True, removes square brackets from title text.

    Returns:
        dict: A new dictionary with the cleaned and restructured data, keyed by pmid.
              Returns an empty dictionary if 'pmid' is not present in the data.
    """
    if 'pmid' not in raw_data:
        print("Error: 'pmid' not found in the input data.")
        return {}

    pmid_key = raw_data['pmid']
    cleaned_data = {}

    # Iterate through all key-value pairs in the input data
    for key, value in raw_data.items():
        if key == 'authors' and isinstance(value, list):
            # Process authors: join forename and lastname
            authors_list = [f"{author.get('forename', '')} {author.get('lastname', '')}".strip() 
                            for author in value]
            cleaned_data['authors'] = authors_list
        
        elif key == 'title':
            # Ensure title is always a string, handling various possible formats
            if isinstance(value, list):
                # Handle potential nested lists by flattening them first
                flat_values = []
                for item in value:
                    if isinstance(item, list):
                        flat_values.extend(item)
                    elif item is not None:
                        flat_values.append(str(item))
                title_str = ' '.join(flat_values) if flat_values else ""
            elif value is None:
                title_str = ""
            else:
                title_str = str(value)
                
            # Optionally remove square brackets from title
            if remove_title_brackets and title_str:
                # Remove square brackets but keep the content inside them
                import re
                title_str = re.sub(r'\[|\]', '', title_str)
                
            cleaned_data[key] = title_str
        
        elif key in ['mesh_terms', 'publication_types', 'chemical_list', 'keywords'] and isinstance(value, str):
            # Process semicolon-separated fields and clean up whitespace
            processed_list = [item.strip() for item in value.split(';') if item.strip()]
            cleaned_data[key] = processed_list
        
        elif key == 'references' and isinstance(value, list):
            # Process references: keep as is, but include a check for the type
            cleaned_data['references'] = value

        elif key == 'grant_ids' and isinstance(value, list):
            # Process grant_ids: keep as is
            cleaned_data['grant_ids'] = value
        
        else:
            # Keep all other fields as is
            cleaned_data[key] = value

    # Create the final dictionary with the pmid as the key
    final_json = {
        pmid_key: cleaned_data
    }

    return final_json

def save_to_json(data, output_file):
    """
    Save parsed data to a JSON file with proper Unicode handling
    
    Args:
        data (list or dict): List or dictionary of data to save
        output_file (str): Output file path
    """
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Data saved to {output_file}")

def main():
    # Define the input file
    input_directory = "/Users/gopinath.balu/Workspace/agentic_ai_innovations/pubmed_baseline2025"
    
    # Define output files
    output_dir = Path("/Users/gopinath.balu/Workspace/agentic_ai_innovations/intermediate_parsed/")
    article_output = output_dir / "pubmed_articles_full.json"
    
    # Testing mode flag - set to True to process only a few files
    testing_mode = False
    test_file_count = 2  # Number of files to process in testing mode
    
    # Configure number of workers for concurrent processing
    # Use a reasonable number based on CPU cores (typically 2-4x number of cores for I/O bound tasks)
    max_workers = os.cpu_count() * 2
    
    # Find all xml.gz files in the input directory
    xml_files = list(Path(input_directory).glob('*.xml.gz'))
    
    if not xml_files:
        print(f"No XML files found in {input_directory}")
        return
    
    # Limit files if in testing mode
    if testing_mode:
        xml_files = xml_files[:test_file_count]
        print(f"TESTING MODE: Processing only {len(xml_files)} files with {max_workers} workers")
    else:
        print(f"Found {len(xml_files)} XML files to process with {max_workers} workers")
    
    # Thread-safe list to store all articles
    all_articles = []
    processed_articles = {}  # Dictionary to store cleaned articles keyed by PMID
    lock = threading.Lock()
    
    # Progress bar setup
    pbar = tqdm(
        total=len(xml_files),
        desc="Processing PubMed files",
        unit="file",
        ncols=100,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
    )
    
    # Callback function to process results from each worker
    def process_result(future):
        try:
            articles, file_path = future.result()
            with lock:
                # Process each article with the cleanup function
                for article in articles:
                    processed = process_publication_data(article, remove_title_brackets=True)
                    if processed:  # Only add if processing was successful
                        pmid = next(iter(processed))  # Get the PMID key
                        processed_articles[pmid] = processed[pmid]
                
                # Delete the file after successful processing
                try:
                    os.remove(file_path)
                    # print(f"Deleted {file_path} to save disk space")
                except Exception as e:
                    print(f"Warning: Failed to delete {file_path}: {e}")
                
                pbar.update(1)
        except Exception as e:
            print(f"Error processing a file: {e}")
            pbar.update(1)
    
    # Use ThreadPoolExecutor for concurrent processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks to the executor
        futures = []
        for xml_file in xml_files:
            future = executor.submit(parse_pubmed_file, str(xml_file))
            future.add_done_callback(process_result)
            futures = futures + [future]
        
        # Wait for all tasks to complete (this is already handled by the context manager)
        # but we'll use concurrent.futures.as_completed to ensure all callbacks finish
        for _ in concurrent.futures.as_completed(futures):
            pass
    
    # Close the progress bar
    pbar.close()
    
    # Save the combined data
    print(f"Processing complete. Total: {len(processed_articles)} articles")
    save_to_json(processed_articles, article_output)

if __name__ == "__main__":
    main()