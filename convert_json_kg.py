import os
import json
import csv

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
    # Use dictionaries to store unique entities, with a structure suitable for CSV headers
    # The structure includes a 'nodeId' to easily identify the node
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
    # Use a dictionary to store relationships, keyed by type
    relationships = {
        'WROTE': [],
        'HAS_MESH_TERM': [],
        'HAS_PUBLICATION_TYPE': [],
        'CONTAINS_CHEMICAL': [],
        'HAS_KEYWORD': [],
        'FUNDED_BY': [],
        'PUBLISHED_IN': [],
        'PUBLISHED_FROM': [],
        'CITES': []
    }

    for pmid, paper_data in json_data.items():
        # --- Create Paper Node ---
        paper_node_id = f"Paper_{pmid}"
        nodes['Paper'][paper_node_id] = {
            'nodeId': paper_node_id,
            'pmid': pmid,
            'title': paper_data.get('title'),
            'abstract': paper_data.get('abstract'),
            'pubdate': paper_data.get('pubdate'),
            'doi': paper_data.get('doi'),
            'pages': paper_data.get('pages'),
            'issue': paper_data.get('issue'),
            'languages': paper_data.get('languages')
        }

        # --- Create Author Nodes and WROTE relationships ---
        authors = paper_data.get('authors', [])
        for author_name in authors:
            author_node_id = f"Author_{author_name.replace(' ', '_').replace('.', '')}"
            nodes['Author'][author_node_id] = {'nodeId': author_node_id, 'name': author_name}
            # startNode=author_name (Author), endNode=pmid (Paper)
            relationships['WROTE'].append({
                'startNode': author_name.replace(' ', '_').replace('.', ''),
                'endNode': pmid
            })

        # --- Create MeshTerm Nodes and HAS_MESH_TERM relationships ---
        mesh_terms = paper_data.get('mesh_terms', [])
        for term_with_id in mesh_terms:
            term_id, term = term_with_id.split(':', 1)
            mesh_node_id = f"{term}_{term_id}"
            nodes['MeshTerm'][mesh_node_id] = {'nodeId': mesh_node_id, 'term': term, 'mesh_id': term_id}
            # startNode=pmid (Paper), endNode=term_id (MeshTerm)
            relationships['HAS_MESH_TERM'].append({
                'startNode': pmid,
                'endNode': term_id
            })

        # --- Create PublicationType Nodes and HAS_PUBLICATION_TYPE relationships ---
        pub_types = paper_data.get('publication_types', [])
        for type_with_id in pub_types:
            type_id, pub_type = type_with_id.split(':', 1)
            pub_type_node_id = f"{pub_type}_{type_id}"
            nodes['PublicationType'][pub_type_node_id] = {'nodeId': pub_type_node_id, 'type': pub_type, 'type_id': type_id}
            # startNode=pmid (Paper), endNode=type_id (PublicationType)
            relationships['HAS_PUBLICATION_TYPE'].append({
                'startNode': pmid,
                'endNode': type_id
            })

        # --- Create Chemical Nodes and CONTAINS_CHEMICAL relationships ---
        chemicals = paper_data.get('chemical_list', [])
        for chem_with_id in chemicals:
            chem_name, chem_id = chem_with_id.split(':', 1)
            chem_node_id = f"Chemical_{chem_id}"
            nodes['Chemical'][chem_node_id] = {'nodeId': chem_node_id, 'name': chem_name, 'chemical_id': chem_id}
            # startNode=pmid (Paper), endNode=chem_id (Chemical)
            relationships['CONTAINS_CHEMICAL'].append({
                'startNode': pmid,
                'endNode': chem_id
            })

        # --- Create Keyword Nodes and HAS_KEYWORD relationships ---
        keywords = paper_data.get('keywords', [])
        for keyword_text in keywords:
            keyword_node_id = f"Keyword_{keyword_text.replace(' ', '_').replace('/', '_')}"
            nodes['Keyword'][keyword_node_id] = {'nodeId': keyword_node_id, 'keyword': keyword_text}
            # startNode=pmid (Paper), endNode=keyword_text (Keyword)
            relationships['HAS_KEYWORD'].append({
                'startNode': pmid,
                'endNode': keyword_text.replace(' ', '_').replace('/', '_')
            })

        # --- Create Grant Nodes and FUNDED_BY relationships ---
        grants = paper_data.get('grant_ids', [])
        for grant_info in grants:
            # If grant_id is not available, combine country and agency as grant_id
            if not grant_info.get('grant_id'):
                grant_id = f"{grant_info.get('country', '')}_{grant_info.get('agency', '')}"
            else:
                grant_id = grant_info.get('grant_id')
                
            grant_node_id = f"Grant_{grant_id}"
            nodes['Grant'][grant_node_id] = {
                'nodeId': grant_node_id,
                'grant_id': grant_id,
                'grant_acronym': grant_info.get('grant_acronym'),
                'country': grant_info.get('country'),
                'agency': grant_info.get('agency')
            }
            # startNode=pmid (Paper), endNode=grant_id (Grant)
            relationships['FUNDED_BY'].append({
                'startNode': pmid,
                'endNode': grant_id
            })

        # --- Create Journal Node and PUBLISHED_IN relationship ---
        journal_name = paper_data.get('journal')
        if journal_name:
            journal_node_id = f"Journal_{journal_name.replace(' ', '_')}"
            nodes['Journal'][journal_node_id] = {
                'nodeId': journal_node_id,
                'name': journal_name,
                'nlm_unique_id': paper_data.get('nlm_unique_id'),
                'issn_linking': paper_data.get('issn_linking'),
                'medline_ta': paper_data.get('medline_ta')
            }
            # startNode=pmid (Paper), endNode=journal_name (Journal)
            relationships['PUBLISHED_IN'].append({
                'startNode': pmid,
                'endNode': journal_name.replace(' ', '_')
            })

        # --- Create Country Node and PUBLISHED_FROM relationship ---
        country_name = paper_data.get('country')
        if country_name:
            country_node_id = f"Country_{country_name.replace(' ', '_')}"
            nodes['Country'][country_node_id] = {'nodeId': country_node_id, 'name': country_name}
            # startNode=pmid (Paper), endNode=country_name (Country)
            relationships['PUBLISHED_FROM'].append({
                'startNode': pmid,
                'endNode': country_name.replace(' ', '_')
            })

        # --- Create CITES relationship for references ---
        references = paper_data.get('references', [])
        for ref_info in references:
            cited_pmid = ref_info.get('pmid')
            if cited_pmid:
                cited_paper_node_id = f"Paper_{cited_pmid}"
                # startNode=pmid (Paper), endNode=cited_pmid (Paper)
                relationships['CITES'].append({
                    'startNode': pmid,
                    'endNode': cited_pmid,
                    'citation_text': ref_info.get('citation')
                })
    
    return nodes, relationships

def write_json_files(nodes, relationships, output_dir):
    """
    Writes the provided nodes and relationships data to a JSON file.

    Args:
        nodes (dict): A dictionary of node data, keyed by node type.
        relationships (dict): A dictionary of relationship data, keyed by type.
        output_dir (str): The directory path to save the JSON file.
    """
    # Flatten the nodes dictionary to a list of unique nodes
    unique_nodes = []
    for node_type, node_dict in nodes.items():
        for node_id, node_data in node_dict.items():
            node = {'id': node_id, 'type': node_type, **node_data}
            unique_nodes.append(node)
            
    # Flatten the relationships dictionary
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

    Args:
        nodes (dict): A dictionary of node data, keyed by node type.
        relationships (dict): A dictionary of relationship data, keyed by type.
        output_dir_nodes (str): The directory path to save the node CSV files.
        output_dir_rels (str): The directory path to save the relationship CSV files.
    """
    # Export nodes to CSV
    for node_type, node_dict in nodes.items():
        if node_dict:
            headers = list(list(node_dict.values())[0].keys())
            with open(os.path.join(output_dir_nodes, f'{node_type.lower()}_nodes.csv'), 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(node_dict.values())
    
    # Export relationships to CSV
    for rel_type, rel_list in relationships.items():
        if rel_list:
            rel_headers = list(rel_list[0].keys())
            with open(os.path.join(output_dir_rels, f'{rel_type.lower()}_rels.csv'), 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=rel_headers)
                writer.writeheader()
                writer.writerows(rel_list)

def main():
    """
    Main function to orchestrate the data processing and export.
    """
    # Define file paths and directory structure
    base_dir = "constructed_KG"
    json_dir = os.path.join(base_dir, "json")
    csv_dir = os.path.join(base_dir, "csv")
    csv_nodes_dir = os.path.join(csv_dir, "nodes")
    csv_rels_dir = os.path.join(csv_dir, "relationships")
    input_file_path = "/Users/gopinath.balu/Workspace/agentic_ai_innovations/intermediate_parsed/pubmed_articles.json"
    
    # Create the output directories if they don't exist
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
    
    # Process the data into graph representation
    nodes, relationships = process_pubmed_data(json_data)
    
    # Count total nodes and relationships
    total_nodes = sum(len(d) for d in nodes.values())
    total_relationships = sum(len(d) for d in relationships.values())

    print("\n--- Knowledge Graph Summary ---")
    print(f"Total number of nodes: {total_nodes}")
    print(f"Total number of relationships: {total_relationships}")
    
    # Count nodes by type
    node_counts = {node_type: len(node_dict) for node_type, node_dict in nodes.items()}
    
    print("\n--- Node Distribution ---")
    for node_type, count in node_counts.items():
        print(f"{node_type}: {count} nodes")
    
    # Count relationships by type
    rel_counts = {rel_type: len(rel_list) for rel_type, rel_list in relationships.items()}
    
    print("\n--- Relationship Distribution ---")
    for rel_type, count in rel_counts.items():
        print(f"{rel_type}: {count} relationships")
    
    # Write the graph data to JSON file
    write_json_files(nodes, relationships, json_dir)
    print(f"\nKnowledge graph saved to JSON file at: {os.path.join(json_dir, 'pubmed_knowledge_graph.json')}")

    # Write the graph data to CSV files
    write_csv_files(nodes, relationships, csv_nodes_dir, csv_rels_dir)
    print(f"Knowledge graph data saved to CSV files in: {csv_dir}")
    
    print("\nKnowledge Graph generation completed successfully.")

if __name__ == "__main__":
    main()
