import os
import json
import csv
import hashlib

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
    Processes a dictionary of PubMed JSON data into a structured knowledge graph
    representation (nodes and relationships) without exporting to CSV. This
    function is for internal data structuring.

    Args:
        json_data (dict): The input JSON data, where keys are PMIDs.

    Returns:
        tuple: A tuple containing two dictionaries: one for all unique nodes and
               one for all relationships, ready for export.
    """
    # Sanitize all keys in the input data to ensure no whitespace
    json_data = sanitize_keys(json_data)
    
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
    relationships = {
        'AUTHORED': [],
        'HAS_MESH_TERM': [],
        'HAS_PUBLICATION_TYPE': [],
        'CONTAINS_CHEMICAL': [],
        'HAS_KEYWORD': [],
        'FUNDED_BY': [],
        'PUBLISHED_IN': [],
        'PUBLISHED_FROM': [],
        'CITES': [],
        # Add inverse relationships for bidirectional connections
        'AUTHOR_OF': [],
        'MESH_TERM_OF': [],
        'PUBLICATION_TYPE_OF': [],
        'CHEMICAL_IN': [],
        'KEYWORD_OF': [],
        'FUNDS': [],
        'PUBLISHES': [],
        'ORIGIN_OF': [],
        'CITED_BY': []
    }

    for pmid, paper_data in json_data.items():
        # --- Create Paper Node using PMID as key ---
        pmid_id = f"Paper_{pmid}"
        nodes['Paper'][pmid_id] = {
            'id': pmid_id,
            'pmid': pmid,
            'title': paper_data.get('title'),
            'abstract': paper_data.get('abstract'),
            'pubdate': paper_data.get('pubdate'),
            'doi': paper_data.get('doi'),
            'pages': paper_data.get('pages'),
            'issue': paper_data.get('issue'),
            'languages': paper_data.get('languages')
        }

        # --- Create Author Nodes and bidirectional AUTHORED/AUTHOR_OF relationships ---
        authors = paper_data.get('authors', [])
        for author_name in authors:
            # Use a deterministic ID as a proxy for ORCID
            author_id = f"Author_{generate_deterministic_id(author_name)}"
            nodes['Author'][author_id] = {'id': author_id, 'name': author_name}
            # Start and end nodes now use the new IDs
            relationships['AUTHORED'].append({
                'startNode': author_id,
                'endNode': pmid_id
            })
            # Add inverse relationship
            relationships['AUTHOR_OF'].append({
                'startNode': pmid_id,
                'endNode': author_id
            })

        # --- Create MeshTerm Nodes and bidirectional HAS_MESH_TERM/MESH_TERM_OF relationships ---
        mesh_terms = paper_data.get('mesh_terms', [])
        for term_with_id in mesh_terms:
            term_id, term = term_with_id.split(':', 1)
            mesh_node_id = f"MeshTerm_{term_id}"
            nodes['MeshTerm'][mesh_node_id] = {'id': mesh_node_id, 'term': term, 'mesh_id': term_id}
            # The relationship uses the standardized Mesh ID
            relationships['HAS_MESH_TERM'].append({
                'startNode': pmid_id,
                'endNode': mesh_node_id
            })
            # Add inverse relationship
            relationships['MESH_TERM_OF'].append({
                'startNode': mesh_node_id,
                'endNode': pmid_id
            })

        # --- Create PublicationType Nodes and bidirectional HAS_PUBLICATION_TYPE/PUBLICATION_TYPE_OF relationships ---
        pub_types = paper_data.get('publication_types', [])
        for type_with_id in pub_types:
            type_id, pub_type = type_with_id.split(':', 1)
            pub_type_node_id = f"PublicationType_{type_id}"
            nodes['PublicationType'][pub_type_node_id] = {'id': pub_type_node_id, 'type': pub_type, 'type_id': type_id}
            # Relationship uses the standardized type ID
            relationships['HAS_PUBLICATION_TYPE'].append({
                'startNode': pmid_id,
                'endNode': pub_type_node_id
            })
            # Add inverse relationship
            relationships['PUBLICATION_TYPE_OF'].append({
                'startNode': pub_type_node_id,
                'endNode': pmid_id
            })

        # --- Create Chemical Nodes and bidirectional CONTAINS_CHEMICAL/CHEMICAL_IN relationships ---
        chemicals = paper_data.get('chemical_list', [])
        for chem_with_id in chemicals:
            chem_id, chem_name = chem_with_id.split(':', 1)
            chem_node_id = f"Chemical_{chem_id}"
            nodes['Chemical'][chem_node_id] = {'id': chem_node_id, 'name': chem_name, 'chemical_id': chem_id}
            # Relationship uses the standardized chemical ID
            relationships['CONTAINS_CHEMICAL'].append({
                'startNode': pmid_id,
                'endNode': chem_node_id
            })
            # Add inverse relationship
            relationships['CHEMICAL_IN'].append({
                'startNode': chem_node_id,
                'endNode': pmid_id
            })

        # --- Create Keyword Nodes and bidirectional HAS_KEYWORD/KEYWORD_OF relationships ---
        keywords = paper_data.get('keywords', [])
        for keyword_text in keywords:
            keyword_id = f"Keyword_{generate_deterministic_id(keyword_text)}"
            nodes['Keyword'][keyword_id] = {'id': keyword_id, 'keyword': keyword_text}
            # Relationship uses the generated ID
            relationships['HAS_KEYWORD'].append({
                'startNode': pmid_id,
                'endNode': keyword_id
            })
            # Add inverse relationship
            relationships['KEYWORD_OF'].append({
                'startNode': keyword_id,
                'endNode': pmid_id
            })

        # --- Create Grant Nodes and bidirectional FUNDED_BY/FUNDS relationships ---
        grants = paper_data.get('grant_ids', [])
        for grant_info in grants:
            grant_id = grant_info.get('grant_id')
            # If grant_id is not available, create a deterministic ID from available info
            if not grant_id:
                intem_grant_key = f"{grant_info.get('country', '')}_{grant_info.get('agency', '')}"
                grant_id = intem_grant_key

            grant_node_id = f"Grant_{grant_id}"
            nodes['Grant'][grant_node_id] = {
                'id': grant_node_id,
                'grant_id': grant_id,
                'grant_acronym': grant_info.get('grant_acronym'),
                'country': grant_info.get('country'),
                'agency': grant_info.get('agency')
            }
            # Relationship uses the grant ID
            relationships['FUNDED_BY'].append({
                'startNode': pmid_id,
                'endNode': grant_node_id
            })
            # Add inverse relationship
            relationships['FUNDS'].append({
                'startNode': grant_node_id,
                'endNode': pmid_id
            })

        # --- Create Journal Node and bidirectional PUBLISHED_IN/PUBLISHES relationship ---
        journal_name = paper_data.get('journal')
        nlm_unique_id = paper_data.get('nlm_unique_id')
        if journal_name:
            # Use NLM Unique ID if available, otherwise use a deterministic hash of the name
            journal_id = nlm_unique_id if nlm_unique_id else generate_deterministic_id(journal_name)
            journal_node_id = f"Journal_{journal_id}"
            nodes['Journal'][journal_node_id] = {
                'id': journal_node_id,
                'name': journal_name,
                'nlm_unique_id': nlm_unique_id,
                'issn_linking': paper_data.get('issn_linking'),
                'medline_ta': paper_data.get('medline_ta')
            }
            # Relationship uses the journal ID
            relationships['PUBLISHED_IN'].append({
                'startNode': pmid_id,
                'endNode': journal_node_id
            })
            # Add inverse relationship
            relationships['PUBLISHES'].append({
                'startNode': journal_node_id,
                'endNode': pmid_id
            })

        # --- Create Country Node and bidirectional PUBLISHED_FROM/ORIGIN_OF relationship ---
        country_name = paper_data.get('country')
        if country_name:
            country_id = generate_deterministic_id(country_name)
            country_node_id = f"Country_{country_id}"
            nodes['Country'][country_node_id] = {'id': country_node_id, 'name': country_name}
            # Relationship uses the country ID
            relationships['PUBLISHED_FROM'].append({
                'startNode': pmid_id,
                'endNode': country_node_id
            })
            # Add inverse relationship
            relationships['ORIGIN_OF'].append({
                'startNode': country_node_id,
                'endNode': pmid_id
            })

        # --- Create bidirectional CITES/CITED_BY relationship for references ---
        references = paper_data.get('references', [])
        for ref_info in references:
            cited_pmid = ref_info.get('pmid')
            if cited_pmid:
                cited_paper_node_id = f"Paper_{cited_pmid}"
                # The relationship uses the standardized PMID
                relationships['CITES'].append({
                    'startNode': pmid_id,
                    'endNode': cited_paper_node_id,
                    'citation_text': ref_info.get('citation')
                })
                # Add inverse relationship
                relationships['CITED_BY'].append({
                    'startNode': cited_paper_node_id,
                    'endNode': pmid_id,
                    'citation_text': ref_info.get('citation')
                })
    
    return nodes, relationships

def write_json_files(nodes, relationships, output_dir):
    """
    Writes the provided nodes and relationships data to a JSON file.
    """
    unique_nodes = []
    for node_type, node_dict in nodes.items():
        for node_id, node_data in node_dict.items():
            node = {'id': node_id, 'type': node_type, **node_data}
            unique_nodes.append(node)
            
    all_relationships = []
    for rel_type, rel_list in relationships.items():
        for rel_data in rel_list:
            all_relationships.append({'type': rel_type, **rel_data})

    kg_representation = {
        'nodes': unique_nodes,
        'relationships': all_relationships
    }

    output_file_path = os.path.join(output_dir, "pubmed_knowledge_graph.json")
    with open(output_file_path, 'w', encoding='utf-8') as f:
        json.dump(kg_representation, f, indent=2, ensure_ascii=False)

def write_csv_files(nodes, relationships, output_dir_nodes, output_dir_rels):
    """
    Writes the provided nodes and relationships data to a set of CSV files.
    """
    for node_type, node_dict in nodes.items():
        if node_dict:
            headers = list(list(node_dict.values())[0].keys())
            file_path = os.path.join(output_dir_nodes, f'{node_type.lower()}_nodes.csv')
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(node_dict.values())
    
    for rel_type, rel_list in relationships.items():
        if rel_list:
            rel_headers = list(rel_list[0].keys())
            file_path = os.path.join(output_dir_rels, f'{rel_type.lower()}_rels.csv')
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=rel_headers)
                writer.writeheader()
                writer.writerows(rel_list)

def main():
    """
    Main function to orchestrate the data processing and export.
    """
    base_dir = "constructed_KG"
    json_dir = os.path.join(base_dir, "json")
    csv_dir = os.path.join(base_dir, "csv")
    csv_nodes_dir = os.path.join(csv_dir, "nodes")
    csv_rels_dir = os.path.join(csv_dir, "relationships")
    input_file_path = "/Users/gopinath.balu/Workspace/agentic_ai_innovations/intermediate_parsed/pubmed_articles.json"
    
    for directory in [json_dir, csv_nodes_dir, csv_rels_dir]:
        os.makedirs(directory, exist_ok=True)

    if not os.path.exists(input_file_path):
        print(f"Error: Input file not found at {input_file_path}")
        return
    
    try:
        with open(input_file_path, 'r', encoding='utf-8') as file:
            json_data = json.load(file)
    except json.JSONDecodeError:
        print(f"Error: Could not parse JSON from {input_file_path}")
        return
    except Exception as e:
        print(f"Error reading file: {str(e)}")
        return
    
    print(f"Successfully loaded data from {input_file_path}")
    print(f"Number of articles loaded: {len(json_data)}")
    
    nodes, relationships = process_pubmed_data(json_data)
    
    total_nodes = sum(len(d) for d in nodes.values())
    total_relationships = sum(len(d) for d in relationships.values())

    print("\n--- Knowledge Graph Summary ---")
    print(f"Total number of nodes: {total_nodes}")
    print(f"Total number of relationships: {total_relationships}")
    
    node_counts = {node_type: len(node_dict) for node_type, node_dict in nodes.items()}
    
    print("\n--- Node Distribution ---")
    for node_type, count in node_counts.items():
        print(f"{node_type}: {count} nodes")
    
    rel_counts = {rel_type: len(rel_list) for rel_type, rel_list in relationships.items()}
    
    print("\n--- Relationship Distribution ---")
    for rel_type, count in rel_counts.items():
        print(f"{rel_type}: {count} relationships")
    
    write_json_files(nodes, relationships, json_dir)
    print(f"\nKnowledge graph saved to JSON file at: {os.path.join(json_dir, 'pubmed_knowledge_graph.json')}")

    write_csv_files(nodes, relationships, csv_nodes_dir, csv_rels_dir)
    print(f"Knowledge graph data saved to CSV files in: {csv_dir}")
    
    print("\nKnowledge Graph generation completed successfully.")

if __name__ == "__main__":
    main()

# Primary Keys for Each Node Type
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