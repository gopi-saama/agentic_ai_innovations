#!/usr/bin/env python3
import json
import gzip
import os
import sys
import argparse
import time
from pathlib import Path
from collections import Counter
# import multiprocessing  # Commented out for non-multiprocessing test
# from functools import partial  # Commented out for non-multiprocessing test

def load_json_data(file_path):
    """
    Load JSON data from a file (plain or gzipped)
    
    Args:
        file_path (str or Path): Path to the JSON file
        
    Returns:
        dict: The loaded JSON data
    """
    file_path = Path(file_path)
    
    if file_path.suffix == '.gz':
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            return json.load(f)
    else:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)

def count_abstracts(data):
    """
    Count articles with abstracts and their properties
    
    Args:
        data (dict): Dictionary of PubMed articles
        
    Returns:
        tuple: (total_articles, articles_with_abstracts, abstract_lengths)
    """
    total_articles = len(data)
    articles_with_abstracts = 0
    abstract_lengths = []
    
    for pmid, article in data.items():
        abstract = article.get('abstract', '')
        if abstract and abstract.strip():
            articles_with_abstracts += 1
            abstract_lengths.append(len(abstract))
    
    return total_articles, articles_with_abstracts, abstract_lengths

# def process_chunk(chunk_data):
#     """
#     Process a chunk of articles data
#     
#     Args:
#         chunk_data (dict): A subset of PubMed articles to process
#         
#     Returns:
#         tuple: (total_articles, articles_with_abstracts, abstract_lengths)
#     """
#     return count_abstracts(chunk_data)

# def split_dict_into_chunks(data, num_chunks):
#     """
#     Split a dictionary into approximately equal chunks
#     
#     Args:
#         data (dict): The dictionary to split
#         num_chunks (int): Number of chunks to create
#         
#     Returns:
#         list: List of dictionaries (chunks)
#     """
#     keys = list(data.keys())
#     chunk_size = max(1, len(keys) // num_chunks)
#     chunks = []
#     
#     for i in range(0, len(keys), chunk_size):
#         chunk_keys = keys[i:i + chunk_size]
#         chunk = {k: data[k] for k in chunk_keys}
#         chunks.append(chunk)
#     
#     return chunks

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Count abstracts in PubMed JSON data (no multiprocessing)')
    parser.add_argument('input_file', help='Path to JSON file (can be gzipped)')
    # parser.add_argument('-p', '--processes', type=int, default=multiprocessing.cpu_count() * 2,
    #                     help='Number of processes to use (default: number of CPU cores)')
    args = parser.parse_args()
    
    # Load the data
    print(f"Loading data from {args.input_file}...")
    start_time = time.time()
    data = load_json_data(args.input_file)
    load_time = time.time() - start_time
    print(f"Data loaded in {load_time:.2f} seconds")
    
    print(f"Processing data without multiprocessing...")
    process_start_time = time.time()
    
    # Direct processing without multiprocessing
    total_articles, articles_with_abstracts, abstract_lengths = count_abstracts(data)
    
    # # For very small datasets, don't use multiprocessing
    # if len(data) < args.processes * 10:
    #     print("Small dataset detected, using single-process mode")
    #     total_articles, articles_with_abstracts, abstract_lengths = count_abstracts(data)
    # else:
    #     # Split data into chunks
    #     chunks = split_dict_into_chunks(data, args.processes)
    #     print(f"Split data into {len(chunks)} chunks")
    #     
    #     # Process chunks in parallel
    #     with multiprocessing.Pool(processes=args.processes) as pool:
    #         results = pool.map(process_chunk, chunks)
    #     
    #     # Combine results
    #     total_articles = sum(r[0] for r in results)
    #     articles_with_abstracts = sum(r[1] for r in results)
    #     abstract_lengths = []
    #     for r in results:
    #         abstract_lengths.extend(r[2])
    
    process_time = time.time() - process_start_time
    print(f"Processing completed in {process_time:.2f} seconds")
    
    # Output results
    print(f"Total articles: {total_articles}")
    print(f"Articles with abstracts: {articles_with_abstracts}")
    print(f"Percentage with abstracts: {articles_with_abstracts/total_articles*100:.2f}%")
    
    if abstract_lengths:
        avg_length = sum(abstract_lengths) / len(abstract_lengths)
        print(f"Average abstract length: {avg_length:.2f} characters")
        print(f"Shortest abstract: {min(abstract_lengths)} characters")
        print(f"Longest abstract: {max(abstract_lengths)} characters")
        
    # Print overall timing summary
    total_time = load_time + process_time
    print(f"\nPerformance Summary:")
    print(f"  - Data loading time: {load_time:.2f} seconds ({load_time/total_time*100:.1f}%)")
    print(f"  - Processing time: {process_time:.2f} seconds ({process_time/total_time*100:.1f}%)")
    print(f"  - Total execution time: {total_time:.2f} seconds")

if __name__ == "__main__":
    main()