import json
import os

def convert_pubmed_json_to_kg(json_data):
    """
    Converts a dictionary of PubMed JSON data into a knowledge graph representation
    based on the defined schema.

    Args:
        json_data (dict): The input JSON data, where keys are PMIDs.

    Returns:
        dict: A dictionary containing 'nodes' and 'relationships' lists.
    """
    # Use sets to store unique entities to avoid duplicates
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
    relationships = []

    for pmid, paper_data in json_data.items():
        # --- Create Paper Node ---
        paper_node_id = f"Paper_{pmid}"
        nodes['Paper'][paper_node_id] = {
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
            nodes['Author'][author_node_id] = {'name': author_name}
            relationships.append({
                'source': author_node_id,
                'target': paper_node_id,
                'type': 'WROTE'
            })

        # --- Create MeshTerm Nodes and HAS_MESH_TERM relationships ---
        mesh_terms = paper_data.get('mesh_terms', [])
        for term_with_id in mesh_terms:
            term, term_id = term_with_id.split(':', 1)
            mesh_node_id = f"MeshTerm_{term_id}"
            nodes['MeshTerm'][mesh_node_id] = {
                'term': term,
                'mesh_id': term_id
            }
            relationships.append({
                'source': paper_node_id,
                'target': mesh_node_id,
                'type': 'HAS_MESH_TERM'
            })

        # --- Create PublicationType Nodes and HAS_PUBLICATION_TYPE relationships ---
        pub_types = paper_data.get('publication_types', [])
        for type_with_id in pub_types:
            pub_type, type_id = type_with_id.split(':', 1)
            pub_type_node_id = f"PublicationType_{type_id}"
            nodes['PublicationType'][pub_type_node_id] = {
                'type': pub_type,
                'type_id': type_id
            }
            relationships.append({
                'source': paper_node_id,
                'target': pub_type_node_id,
                'type': 'HAS_PUBLICATION_TYPE'
            })

        # --- Create Chemical Nodes and CONTAINS_CHEMICAL relationships ---
        chemicals = paper_data.get('chemical_list', [])
        for chem_with_id in chemicals:
            chem_name, chem_id = chem_with_id.split(':', 1)
            chem_node_id = f"Chemical_{chem_id}"
            nodes['Chemical'][chem_node_id] = {
                'name': chem_name,
                'chemical_id': chem_id
            }
            relationships.append({
                'source': paper_node_id,
                'target': chem_node_id,
                'type': 'CONTAINS_CHEMICAL'
            })

        # --- Create Keyword Nodes and HAS_KEYWORD relationships ---
        keywords = paper_data.get('keywords', [])
        for keyword_text in keywords:
            keyword_node_id = f"Keyword_{keyword_text.replace(' ', '_').replace('/', '_')}"
            nodes['Keyword'][keyword_node_id] = {'keyword': keyword_text}
            relationships.append({
                'source': paper_node_id,
                'target': keyword_node_id,
                'type': 'HAS_KEYWORD'
            })

        # --- Create Grant Nodes and FUNDED_BY relationships ---
        grants = paper_data.get('grant_ids', [])
        for grant_info in grants:
            grant_node_id = f"Grant_{grant_info['grant_id']}"
            nodes['Grant'][grant_node_id] = {
                'grant_id': grant_info.get('grant_id'),
                'grant_acronym': grant_info.get('grant_acronym'),
                'country': grant_info.get('country'),
                'agency': grant_info.get('agency')
            }
            relationships.append({
                'source': paper_node_id,
                'target': grant_node_id,
                'type': 'FUNDED_BY'
            })

        # --- Create Journal Node and PUBLISHED_IN relationship ---
        journal_name = paper_data.get('journal')
        if journal_name:
            journal_node_id = f"Journal_{journal_name.replace(' ', '_')}"
            nodes['Journal'][journal_node_id] = {
                'name': journal_name,
                'nlm_unique_id': paper_data.get('nlm_unique_id'),
                'issn_linking': paper_data.get('issn_linking'),
                'medline_ta': paper_data.get('medline_ta')
            }
            relationships.append({
                'source': paper_node_id,
                'target': journal_node_id,
                'type': 'PUBLISHED_IN'
            })

        # --- Create Country Node and PUBLISHED_FROM relationship ---
        country_name = paper_data.get('country')
        if country_name:
            country_node_id = f"Country_{country_name.replace(' ', '_')}"
            nodes['Country'][country_node_id] = {'name': country_name}
            relationships.append({
                'source': paper_node_id,
                'target': country_node_id,
                'type': 'PUBLISHED_FROM'
            })

        # --- Create CITES relationship for references ---
        references = paper_data.get('references', [])
        for ref_info in references:
            cited_pmid = ref_info.get('pmid')
            if cited_pmid:
                cited_paper_node_id = f"Paper_{cited_pmid}"
                # Create a relationship regardless of whether the cited paper is in the current dataset
                # A graph database would handle the existence of the node on its own
                relationships.append({
                    'source': paper_node_id,
                    'target': cited_paper_node_id,
                    'type': 'CITES',
                    'properties': {'citation_text': ref_info.get('citation')}
                })

    # Flatten the nodes dictionary to a list of unique nodes
    unique_nodes = []
    for node_type, node_dict in nodes.items():
        for node_id, node_data in node_dict.items():
            node = {'id': node_id, 'type': node_type, **node_data}
            unique_nodes.append(node)
    
    return {
        'nodes': unique_nodes,
        'relationships': relationships
    }

def main():
    # Define the input file path
    input_file_path = "/Users/gopinath.balu/Workspace/agentic_ai_innovations/pubmed_articles.json"
    
    # Check if the file exists
    if not os.path.exists(input_file_path):
        print(f"Error: Input file not found at {input_file_path}")
        return
    
    # Load the JSON data from file
    try:
        with open(input_file_path, 'r') as file:
            json_data = json.load(file)
    except json.JSONDecodeError:
        print(f"Error: Could not parse JSON from {input_file_path}")
        return
    except Exception as e:
        print(f"Error reading file: {str(e)}")
        return
    
    print(f"Successfully loaded data from {input_file_path}")
    print(f"Number of articles loaded: {len(json_data)}")
    
    # Convert the data to knowledge graph format
    kg_representation = convert_pubmed_json_to_kg(json_data)
    
    # Print information about the knowledge graph
    print("\n--- Knowledge Graph Summary ---")
    print(f"Total number of nodes: {len(kg_representation['nodes'])}")
    print(f"Total number of relationships: {len(kg_representation['relationships'])}")
    
    # Count nodes by type
    node_types = {}
    for node in kg_representation['nodes']:
        node_type = node['type']
        if node_type in node_types:
            node_types[node_type] += 1
        else:
            node_types[node_type] = 1
    
    print("\n--- Node Distribution ---")
    for node_type, count in node_types.items():
        print(f"{node_type}: {count} nodes")
    
    # Count relationships by type
    rel_types = {}
    for rel in kg_representation['relationships']:
        rel_type = rel['type']
        if rel_type in rel_types:
            rel_types[rel_type] += 1
        else:
            rel_types[rel_type] = 1
    
    print("\n--- Relationship Distribution ---")
    for rel_type, count in rel_types.items():
        print(f"{rel_type}: {count} relationships")
    
    # Save the knowledge graph to a file
    output_file_path = "/Users/gopinath.balu/Workspace/agentic_ai_innovations/pubmed_knowledge_graph.json"
    with open(output_file_path, 'w', encoding='utf-8') as f:
        json.dump(kg_representation, f, indent=2, ensure_ascii=False)
    print(f"Knowledge graph saved to {output_file_path}")
    
    print("\nKnowledge Graph generation completed successfully.")

if __name__ == "__main__":
    main()
