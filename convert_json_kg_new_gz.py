import os
import json
import csv
import hashlib
import gzip
import time  # Import time module for timing
from tqdm import tqdm  # Import tqdm for progress tracking
import multiprocessing as mp
import glob
import argparse
from functools import partial

def generate_deterministic_id(text):
    """
    Generates a consistent, unique ID for entities without a standard ID
    by hashing their text content.
    """
    if not text:
        return None
    # Use a secure hash to create a short, consistent identifier
    return hashlib.sha1(text.encode('utf-8')).hexdigest()[:10]

def sanitize_keys(data):
    """
    Recursively sanitize dictionary keys by replacing any whitespace with underscore.
    Works with nested dictionaries and lists containing dictionaries.
    
    Args:
        data: Dictionary, list, or any other data structure to sanitize
        
    Returns:
        The sanitized data structure with whitespace in keys replaced by underscores
    """
    if isinstance(data, dict):
        # Create a new dictionary with sanitized keys
        sanitized_dict = {}
        for key, value in data.items():
            # Replace whitespace with underscore in the key
            sanitized_key = key.replace(" ", "_") if isinstance(key, str) else key
            # Recursively sanitize the value
            sanitized_dict[sanitized_key] = sanitize_keys(value)
        return sanitized_dict
    elif isinstance(data, list):
        # Recursively sanitize each item in the list
        return [sanitize_keys(item) for item in data]
    else:
        # Return primitive values as is
        return data

def process_pubmed_data(json_data):
    """
    Process the PubMed JSON data to build a knowledge graph.
    
    Args:
        json_data: Dictionary of PMID -> article data
    
    Returns:
        Tuple of (nodes, relationships) where:
        - nodes is a dict of node_type -> node_id -> node_data
        - relationships is a dict of rel_type -> list of relationships
    """
    # Start timing the core processing
    core_start = time.time()
    
    # Initialize dictionaries to store nodes and relationships
    nodes = {
        'Paper': {},
        'Author': {},
        'MeshTerm': {},
        'PublicationType': {},
        'Chemical': {},
        'Keyword': {},
        'Grant': {},
        'Journal': {},
        'Country': {}
    }
    
    # Define relationship types
    relationships = {
        'AUTHORED': [],
        'HAS_MESH_TERM': [],
        'HAS_PUBLICATION_TYPE': [],
        'CONTAINS_CHEMICAL': [],
        'HAS_KEYWORD': [],
        'FUNDED_BY': [],
        'PUBLISHED_IN': [],
        'PUBLISHED_FROM': [],
        'CITES': []
    }
    
    sub_timings = {
        'nodes_creation': 0,
        'relationships_creation': 0,
        'papers_processing': 0
    }
    
    # Process each paper
    papers_start = time.time()
    for pmid, paper_data in tqdm(json_data.items(), desc="Processing papers"):
        # Start by creating the paper node
        title = paper_data.get('title', '')
        
        # If title is a list, join it into a single string
        if isinstance(title, list):
            title = ' '.join(title)
            
        abstract = paper_data.get('abstract', '')
        year = paper_data.get('year', '')
        
        # Create Paper node with standardized ID
        pmid_id = f"Paper_{pmid}"
        nodes['Paper'][pmid_id] = {
            'id': pmid_id,
            'pmid': pmid,
            'title': title,
            'abstract': abstract,
            'year': year
        }
        
        # Start timing relationship creation for this paper
        rel_start = time.time()
        
        # Create Author nodes and AUTHORED relationships
        authors = paper_data.get('authors', [])
        for author_name in authors:
            # Use deterministic ID for authors based on name
            author_id = f"Author_{generate_deterministic_id(author_name)}"
            
            # Create Author node if not exists
            if author_id not in nodes['Author']:
                nodes['Author'][author_id] = {
                    'id': author_id,
                    'name': author_name
                }
            
            # Create AUTHORED relationship
            relationships['AUTHORED'].append({
                'startNode': author_id,
                'endNode': pmid_id
            })
        
        # Create MeshTerm nodes and HAS_MESH_TERM relationships
        mesh_terms = paper_data.get('mesh_terms', [])
        for mesh_with_id in mesh_terms:
            # Split the MeSH term ID and name (format: "D000001:Term Name")
            mesh_id, mesh_name = mesh_with_id.split(':', 1)
            
            mesh_node_id = f"MeshTerm_{mesh_id}"
            
            # Create MeshTerm node if not exists
            if mesh_node_id not in nodes['MeshTerm']:
                nodes['MeshTerm'][mesh_node_id] = {
                    'id': mesh_node_id,
                    'name': mesh_name,
                    'mesh_id': mesh_id
                }
            
            # Create HAS_MESH_TERM relationship
            relationships['HAS_MESH_TERM'].append({
                'startNode': pmid_id,
                'endNode': mesh_node_id
            })
        
        # Create PublicationType nodes and HAS_PUBLICATION_TYPE relationships
        pub_types = paper_data.get('publication_types', [])
        for pub_type_with_id in pub_types:
            # Split the publication type ID and name
            pub_type_id, pub_type_name = pub_type_with_id.split(':', 1)
            pub_type_node_id = f"PublicationType_{pub_type_id}"
            
            # Create PublicationType node if not exists
            if pub_type_node_id not in nodes['PublicationType']:
                nodes['PublicationType'][pub_type_node_id] = {
                    'id': pub_type_node_id,
                    'name': pub_type_name,
                    'publication_type_id': pub_type_id
                }
            
            # Create HAS_PUBLICATION_TYPE relationship
            relationships['HAS_PUBLICATION_TYPE'].append({
                'startNode': pmid_id,
                'endNode': pub_type_node_id
            })
        
        # --- Create Chemical Nodes and CONTAINS_CHEMICAL relationships ---
        chemicals = paper_data.get('chemical_list', [])
        for chem_with_id in chemicals:
            try:
                # Only process chemical entries with the standard ID:Name format
                if ':' in chem_with_id:
                    # Standard format with ID:Name
                    chem_id, chem_name = chem_with_id.split(':', 1)
                    chem_node_id = f"Chemical_{chem_id}"
                    
                    # Create the chemical node
                    nodes['Chemical'][chem_node_id] = {
                        'id': chem_node_id, 
                        'name': chem_name, 
                        'chemical_id': chem_id
                    }
                    
                    # Create the relationship
                    relationships['CONTAINS_CHEMICAL'].append({
                        'startNode': pmid_id,
                        'endNode': chem_node_id
                    })
                # Skip entries without the ID:Name format silently
            except Exception as e:
                # Log error but continue processing
                print(f"Error processing chemical for PMID {pmid}: {str(e)}")
                continue
        
        # --- Create Keyword Nodes and HAS_KEYWORD relationships ---
        keywords = paper_data.get('keywords', [])
        for keyword in keywords:
            # Use deterministic ID for keywords
            keyword_id = generate_deterministic_id(keyword)
            keyword_node_id = f"Keyword_{keyword_id}"
            
            # Create Keyword node if not exists
            if keyword_node_id not in nodes['Keyword']:
                nodes['Keyword'][keyword_node_id] = {
                    'id': keyword_node_id,
                    'name': keyword,
                    'keyword_id': keyword_id
                }
            
            # Create HAS_KEYWORD relationship
            relationships['HAS_KEYWORD'].append({
                'startNode': pmid_id,
                'endNode': keyword_node_id
            })
        
        # --- Create Grant Nodes and FUNDED_BY relationships ---
        grants = paper_data.get('grant_ids', [])
        for grant_info in grants:
            # Check if grant_info is a dictionary with required fields
            if isinstance(grant_info, dict) and 'id' in grant_info:
                grant_id = grant_info['id']
                # Generate a node ID for this grant
                grant_node_id = f"Grant_{grant_id}"
                
                # Create Grant node
                nodes['Grant'][grant_node_id] = {
                    'id': grant_node_id,
                    'grant_id': grant_id,
                    'agency': grant_info.get('agency', ''),
                    'country': grant_info.get('country', '')
                }
                
                # Create FUNDED_BY relationship
                relationships['FUNDED_BY'].append({
                    'startNode': pmid_id,
                    'endNode': grant_node_id
                })
        
        # --- Create Journal Node and PUBLISHED_IN relationship ---
        journal_data = paper_data.get('journal', {})
        
        # Handle journal data which can be either a string or a dictionary
        if isinstance(journal_data, str):
            # If it's just a string, use it as the journal name
            journal_name = journal_data
            journal_id = generate_deterministic_id(journal_name)
            journal_node_id = f"Journal_{journal_id}"
            
            # Create Journal node
            if journal_node_id not in nodes['Journal']:
                nodes['Journal'][journal_node_id] = {
                    'id': journal_node_id,
                    'name': journal_name,
                    'journal_id': journal_id,
                    'nlm_unique_id': ''
                }
        else:
            # If it's a dictionary, extract relevant fields
            journal_name = journal_data.get('title', '')
            nlm_id = journal_data.get('nlm_unique_id', '')
            
            # Use NLM ID if available, otherwise generate deterministic ID
            journal_id = nlm_id if nlm_id else generate_deterministic_id(journal_name)
            journal_node_id = f"Journal_{journal_id}"
            
            # Create Journal node
            if journal_node_id not in nodes['Journal']:
                nodes['Journal'][journal_node_id] = {
                    'id': journal_node_id,
                    'name': journal_name,
                    'journal_id': journal_id,
                    'nlm_unique_id': nlm_id,
                    'issn': journal_data.get('issn', ''),
                    'issn_type': journal_data.get('issn_type', '')
                }
        
        # Create PUBLISHED_IN relationship
        if journal_node_id:
            relationships['PUBLISHED_IN'].append({
                'startNode': pmid_id,
                'endNode': journal_node_id
            })
        
        # --- Create Country Node and PUBLISHED_FROM relationship ---
        country = paper_data.get('country', '')
        if country:
            country_id = generate_deterministic_id(country)
            country_node_id = f"Country_{country_id}"
            
            # Create Country node
            if country_node_id not in nodes['Country']:
                nodes['Country'][country_node_id] = {
                    'id': country_node_id,
                    'name': country,
                    'country_id': country_id
                }
            
            # Create PUBLISHED_FROM relationship
            relationships['PUBLISHED_FROM'].append({
                'startNode': pmid_id,
                'endNode': country_node_id
            })
        
        # --- Create CITES relationships ---
        references = paper_data.get('references', [])
        for ref_pmid in references:
            # Only create relationship if the referenced paper is in our dataset
            ref_pmid_id = f"Paper_{ref_pmid}"
            
            # Create CITES relationship
            relationships['CITES'].append({
                'startNode': pmid_id,
                'endNode': ref_pmid_id
            })
        
        sub_timings['relationships_creation'] += time.time() - rel_start
    
    sub_timings['papers_processing'] = time.time() - papers_start
    core_time = time.time() - core_start
    
    # Print sub-timings
    print("\n--- Processing Time Breakdown ---")
    print(f"Total papers processing time: {sub_timings['papers_processing']:.2f} seconds")
    print(f"Relationship creation time:   {sub_timings['relationships_creation']:.2f} seconds ({(sub_timings['relationships_creation']/core_time)*100:.2f}% of core processing)")
    
    return nodes, relationships

def process_pubmed_file(file_path, desc=None):
    """
    Process a single PubMed JSON file.
    
    Args:
        file_path: Path to the gzipped JSON file to process
        desc: Description for the tqdm progress bar
    
    Returns:
        Tuple of (nodes, relationships) from the processed file
    """
    print(f"Processing file: {file_path}")
    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as file:
            json_data = json.load(file)
    except json.JSONDecodeError:
        print(f"Error: Could not parse JSON from {file_path}")
        return None
    except Exception as e:
        print(f"Error reading file {file_path}: {str(e)}")
        return None
    
    print(f"Successfully loaded data from {file_path}")
    print(f"Number of articles loaded: {len(json_data)}")
    
    # Process the data
    return process_pubmed_data(json_data)

def write_csv_files_for_file(nodes, relationships, output_dir_nodes, output_dir_rels, file_id):
    """
    Writes the provided nodes and relationships data to a set of CSV files for a single processed file.
    
    Args:
        nodes: Dictionary of node types to node dictionaries
        relationships: Dictionary of relationship types to lists of relationships
        output_dir_nodes: Directory to write node CSV files
        output_dir_rels: Directory to write relationship CSV files
        file_id: Identifier for the file being processed (used in output filenames)
    """
    # Create file_id specific directories
    file_nodes_dir = os.path.join(output_dir_nodes, file_id)
    file_rels_dir = os.path.join(output_dir_rels, file_id)
    
    # Ensure directories exist
    os.makedirs(file_nodes_dir, exist_ok=True)
    os.makedirs(file_rels_dir, exist_ok=True)
    
    # Write node CSV files
    for node_type, node_dict in nodes.items():
        if not node_dict:
            continue
            
        output_path = os.path.join(file_nodes_dir, f"{node_type.lower()}_nodes.csv")
        
        # Extract headers from the first node
        headers = list(next(iter(node_dict.values())).keys())
        
        with open(output_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            writer.writeheader()
            
            for node in node_dict.values():
                writer.writerow(node)
    
    # Write relationship CSV files
    for rel_type, rel_list in relationships.items():
        if not rel_list:
            continue
            
        output_path = os.path.join(file_rels_dir, f"{rel_type.lower()}_relationships.csv")
        
        # Extract headers from the first relationship
        if rel_list:
            headers = list(rel_list[0].keys())
            
            with open(output_path, 'w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=headers)
                writer.writeheader()
                
                for rel in rel_list:
                    writer.writerow(rel)

def process_and_save_file(file_path, output_dirs):
    """
    Process a single PubMed file and save its results directly.
    
    Args:
        file_path: Path to the gzipped JSON file to process
        output_dirs: Dictionary containing output directories
        
    Returns:
        Dict with statistics about the processed file
    """
    file_name = os.path.basename(file_path)
    file_id = os.path.splitext(os.path.splitext(file_name)[0])[0]  # Remove both .json and .gz extensions
    
    try:
        # Process the file
        result = process_pubmed_file(file_path)
        
        if result is None:
            print(f"Error processing file {file_path}")
            return None
            
        nodes, relationships = result
        
        # Generate statistics for this file
        file_node_count = sum(len(d) for d in nodes.values())
        file_rel_count = sum(len(d) for d in relationships.values())
        
        print(f"\nFile {file_id} processed: {file_node_count} nodes, {file_rel_count} relationships")
        
        # Save results for this file
        write_csv_files_for_file(
            nodes, 
            relationships, 
            output_dirs['csv_nodes_dir'], 
            output_dirs['csv_rels_dir'],
            file_id
        )
        
        print(f"Data for {file_id} saved to CSV files")
        
        # Return statistics
        return {
            'file_id': file_id,
            'node_count': file_node_count,
            'rel_count': file_rel_count,
            'node_types': {node_type: len(nodes[node_type]) for node_type in nodes},
            'rel_types': {rel_type: len(relationships[rel_type]) for rel_type in relationships}
        }
        
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        return None

def merge_data(results):
    """
    Merge multiple node and relationship dictionaries into a single set
    
    Args:
        results: List of (nodes, relationships) tuples from processing multiple files
    
    Returns:
        Tuple of merged (nodes, relationships)
    """
    # Initialize the merged dictionaries
    merged_nodes = {
        'Paper': {},
        'Author': {},
        'MeshTerm': {},
        'PublicationType': {},
        'Chemical': {},
        'Keyword': {},
        'Grant': {},
        'Journal': {},
        'Country': {}
    }
    
    merged_relationships = {
        'AUTHORED': [],
        'HAS_MESH_TERM': [],
        'HAS_PUBLICATION_TYPE': [],
        'CONTAINS_CHEMICAL': [],
        'HAS_KEYWORD': [],
        'FUNDED_BY': [],
        'PUBLISHED_IN': [],
        'PUBLISHED_FROM': [],
        'CITES': []
    }
    
    # Merge all results
    for nodes, relationships in results:
        if nodes is None or relationships is None:
            continue
            
        # Merge nodes
        for node_type, node_dict in nodes.items():
            merged_nodes[node_type].update(node_dict)
        
        # Merge relationships
        for rel_type, rel_list in relationships.items():
            merged_relationships[rel_type].extend(rel_list)
    
    return merged_nodes, merged_relationships

def write_json_files(nodes, relationships, output_dir):
    """
    Writes the provided nodes and relationships data to a JSON file.
    """
    print("Converting node dictionaries to lists...")
    # Convert node dictionaries to lists
    nodes_lists = {node_type: list(node_dict.values()) for node_type, node_dict in nodes.items()}
    
    print("Preparing final knowledge graph structure...")
    # Prepare the final knowledge graph structure
    knowledge_graph = {
        'nodes': nodes_lists,
        'relationships': relationships
    }
    
    # Write to file
    output_path = os.path.join(output_dir, 'pubmed_knowledge_graph.json')
    print(f"Writing knowledge graph to {output_path}...")
    with open(output_path, 'w', encoding='utf-8') as file:
        json.dump(knowledge_graph, file)
    print("JSON file writing complete.")

def write_csv_files(nodes, relationships, output_dir_nodes, output_dir_rels):
    """
    Writes the provided nodes and relationships data to a set of CSV files.
    """
    # Write node CSV files
    for node_type, node_dict in nodes.items():
        if not node_dict:
            continue
            
        output_path = os.path.join(output_dir_nodes, f"{node_type.lower()}_nodes.csv")
        
        # Extract headers from the first node
        headers = list(next(iter(node_dict.values())).keys())
        
        with open(output_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            writer.writeheader()
            
            for node in node_dict.values():
                writer.writerow(node)
    
    # Write relationship CSV files
    for rel_type, rel_list in relationships.items():
        if not rel_list:
            continue
            
        output_path = os.path.join(output_dir_rels, f"{rel_type.lower()}_relationships.csv")
        
        # Extract headers from the first relationship
        if rel_list:
            headers = list(rel_list[0].keys())
            
            with open(output_path, 'w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=headers)
                writer.writeheader()
                
                for rel in rel_list:
                    writer.writerow(rel)

def main():
    """
    Main function to orchestrate the data processing and export.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process PubMed files to build a knowledge graph.')
    parser.add_argument('--input', '-i', type=str, default='/Users/gopinath.balu/Workspace/agentic_ai_innovations/intermediate_parsed',
                        help='Input directory containing gzipped JSON files or path pattern (e.g., "/path/to/files/*.json.gz")')
    parser.add_argument('--output', '-o', type=str, default='/Users/gopinath.balu/Workspace/agentic_ai_innovations/constructed_KG_new',
                        help='Base output directory for processed data')
    parser.add_argument('--workers', '-w', type=int, default=mp.cpu_count()//2,
                        help='Number of worker processes for multiprocessing')
    args = parser.parse_args()
    
    start_time = time.time()
    
    # Setup output directories
    base_dir = args.output
    json_dir = os.path.join(base_dir, "json")
    csv_dir = os.path.join(base_dir, "csv")
    csv_nodes_dir = os.path.join(csv_dir, "nodes")
    csv_rels_dir = os.path.join(csv_dir, "relationships")
    
    for directory in [json_dir, csv_nodes_dir, csv_rels_dir]:
        os.makedirs(directory, exist_ok=True)
    
    # Create output directories dictionary to pass to process_and_save_file
    output_dirs = {
        'json_dir': json_dir,
        'csv_nodes_dir': csv_nodes_dir,
        'csv_rels_dir': csv_rels_dir
    }
    
    # Find all input files
    input_files = []
    if os.path.isdir(args.input):
        input_files = glob.glob(os.path.join(args.input, "*.json.gz"))
    else:
        input_files = glob.glob(args.input)
    
    if not input_files:
        print(f"Error: No input files found matching {args.input}")
        return
    
    print(f"Found {len(input_files)} files to process")
    
    # Set up multiprocessing pool
    pool = mp.Pool(processes=args.workers)
    
    # Process files in parallel with progress bar, one at a time
    try:
        # Create partial function with fixed output_dirs parameter
        process_func = partial(process_and_save_file, output_dirs=output_dirs)
        
        # Process and collect statistics for each file
        file_stats = []
        for stat in tqdm(pool.imap_unordered(process_func, input_files), 
                         total=len(input_files), 
                         desc="Processing files"):
            if stat:
                file_stats.append(stat)
    finally:
        pool.close()
        pool.join()
    
    if not file_stats:
        print("Error: No files were processed successfully.")
        return
    
    # Summarize the overall statistics
    total_nodes = sum(stat['node_count'] for stat in file_stats)
    total_relationships = sum(stat['rel_count'] for stat in file_stats)
    
    print("\n--- Knowledge Graph Summary ---")
    print(f"Total number of nodes across all files: {total_nodes}")
    print(f"Total number of relationships across all files: {total_relationships}")
    print(f"Total number of files processed: {len(file_stats)}")
    
    end_time = time.time()
    print(f"\nTotal processing time: {end_time - start_time:.2f} seconds")
    
    print("\nKnowledge Graph generation completed successfully.")

if __name__ == "__main__":
    main()# Primary Keys for Each Node Type
# Node Type	        Primary Key (id field value)	                                
# Paper	            Paper_{pmid}	                                                
# Author	        Author_{deterministic_hash}	                                    
# MeshTerm	        MeshTerm_{mesh_id}	                                            
# PublicationType	PublicationType_{type_id}	                                    
# Chemical	        Chemical_{chemical_id}	                                        
# Keyword	        Keyword_{deterministic_hash}	                                
# Grant	            Grant_{grant_id}                            	                
# Journal	        Journal_{nlm_unique_id}                                         
# Country	        Country_{deterministic_hash}	                                


# Node and Relationship Keys
# The core principle is simple: every node must have a unique primary key. 
# In your modified code, this primary key is stored in the id property of each node object.

# A relationship is then defined by connecting two of these unique identifiers. 
# A relationship object will contain a startNode key and an endNode key, 
# with their values being the **id**s of the respective nodes.

# Mapping the Relationships
# Here is a detailed breakdown of how each relationship type maps the unique primary keys to connect different node types within the graph. 
# This table illustrates which key from the starting node is mapped to which key in the ending node for each relationship.

# Relationship Type	        Start Node (id field)	        End Node (id field)
# AUTHORED	                Author_{deterministic_hash}	    Paper_{pmid}
# HAS_MESH_TERM	            Paper_{pmid}	                MeshTerm_{mesh_id}
# HAS_PUBLICATION_TYPE	    Paper_{pmid}	                PublicationType_{type_id}
# CONTAINS_CHEMICAL	        Paper_{pmid}	                Chemical_{chemical_id}
# HAS_KEYWORD	            Paper_{pmid}	                Keyword_{deterministic_hash}
# FUNDED_BY	                Paper_{pmid}	                Grant_{grant_id or deterministic_hash}
# PUBLISHED_IN	            Paper_{pmid}	                Journal_{nlm_unique_id or deterministic_hash}
# PUBLISHED_FROM	        Paper_{pmid}	                Country_{deterministic_hash}
# CITES	                    Paper_{pmid}	                Paper_{cited_pmid}