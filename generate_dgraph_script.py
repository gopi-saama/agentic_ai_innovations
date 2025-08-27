import json
import os
import sys

def generate_dgraph_schema_and_mutations(input_json_path, output_dir):
    """
    Reads a knowledge graph from a JSON file, generates a Dgraph schema,
    and creates a series of Dgraph mutation scripts for upserting the data.

    Args:
        input_json_path (str): Path to the input JSON file containing the KG.
        output_dir (str): Directory to save the generated schema and mutations.
    """
    if not os.path.exists(input_json_path):
        print(f"Error: Input file not found at {input_json_path}")
        sys.exit(1)

    try:
        with open(input_json_path, 'r', encoding='utf-8') as f:
            kg_data = json.load(f)
    except json.JSONDecodeError:
        print(f"Error: Could not parse JSON from {input_json_path}")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file: {str(e)}")
        sys.exit(1)

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # --- 1. Generate Dgraph Schema ---
    schema_content = """
# Define Node Types (predicates are their properties)
type Paper {
    pmid
    title
    abstract
    pubdate
    doi
    pages
    issue
    languages
    has_author
    has_mesh_term
    has_pub_type
    contains_chemical
    has_keyword
    funded_by
    published_in
    published_from
    cites_paper
}

type Author {
    name
    authored_paper
}

type MeshTerm {
    term
    mesh_id
    used_in_paper
}

type PublicationType {
    type_name: string @lang
    type_id: string @index(exact)
    used_in_paper
}

type Chemical {
    name
    chemical_id
    used_in_paper
}

type Keyword {
    keyword: string @index(term)
    used_in_paper
}

type Grant {
    grant_id: string @index(exact)
    grant_acronym
    country
    agency
    funds_paper
}

type Journal {
    name: string @index(exact)
    nlm_unique_id
    issn_linking
    medline_ta
    publishes_paper
}

type Country {
    name: string @index(exact)
    publishes_from
}

# Define Relationships (predicates connecting types)
# Note: Dgraph relationships are typed, so we define the type of the relationship itself
has_author: [Author] @reverse
has_mesh_term: [MeshTerm] @reverse
has_pub_type: [PublicationType] @reverse
contains_chemical: [Chemical] @reverse
has_keyword: [Keyword] @reverse
funded_by: [Grant] @reverse
published_in: [Journal] @reverse
published_from: [Country] @reverse
cites_paper: [Paper] @reverse
    """
    schema_file_path = os.path.join(output_dir, "dgraph_schema.graphql")
    with open(schema_file_path, 'w', encoding='utf-8') as f:
        f.write(schema_content)
    print(f"Dgraph schema saved to {schema_file_path}")

    # --- 2. Generate Dgraph Mutations (Data) ---
    mutations_file_path = os.path.join(output_dir, "dgraph_mutations.graphql")
    
    unique_nodes = kg_data.get('nodes', [])
    relationships = kg_data.get('relationships', [])

    # Group nodes by type for efficient lookup
    nodes_by_type = {}
    for node in unique_nodes:
        node_type = node.get('type')
        if node_type not in nodes_by_type:
            nodes_by_type[node_type] = {}
        nodes_by_type[node_type][node['id']] = node

    with open(mutations_file_path, 'w', encoding='utf-8') as f:
        # Use a list to store all mutation blocks
        mutation_blocks = []

        # Mutation 1: Create or find all nodes (upsert)
        for node in unique_nodes:
            node_type = node.get('type')
            node_id = node.get('id')
            
            # Upsert block
            mutation = f"""
{{
    # Find existing node or create a new one
    q(func: eq(nodeId, "{node_id}")) {{
        v as uid
    }}
}}
mutation {{
    set {{
        # Upsert logic
        uid(v) <dgraph.type> "{node_type}" .
        <_:newnode> <nodeId> "{node_id}" .
        <_:newnode> <dgraph.type> "{node_type}" .
"""
            # Add properties to the mutation block
            for key, value in node.items():
                if key not in ['id', 'type', 'nodeId'] and value:
                    # Escape double quotes in string values
                    safe_value = str(value).replace('"', '\\"')
                    mutation += f'        <_:newnode> <{key}> "{safe_value}" . \n'
            
            # Close the upsert block
            mutation += """
    }}
}
"""
            mutation_blocks.append(mutation)
        
        # Mutation 2: Create all relationships (upsert)
        for rel in relationships:
            rel_type = rel['type']
            start_node_id = rel['startNode']
            end_node_id = rel['endNode']

            # Find UIDs for both start and end nodes
            rel_mutation = f"""
{{
    q_start(func: eq(nodeId, "{start_node_id}")) {{
        start_uid as uid
    }}
    q_end(func: eq(nodeId, "{end_node_id}")) {{
        end_uid as uid
    }}
}}
mutation {{
    set {{
        # Connect nodes with a relationship
        uid(start_uid) <{rel_type}> uid(end_uid) .
"""
            # Add relationship properties if they exist
            if 'citation_text' in rel:
                safe_citation_text = rel['citation_text'].replace('"', '\\"')
                rel_mutation += f'        uid(start_uid) <citation_text> "{safe_citation_text}" . \n'
            
            rel_mutation += """
    }}
}
"""
            mutation_blocks.append(rel_mutation)

        f.write("\n\n".join(mutation_blocks))
        
    print(f"Dgraph mutations saved to {mutations_file_path}")
    print("\nScript completed. You can now use these files to load data into Dgraph.")

if __name__ == "__main__":
    # Ensure the script is run with the correct path to the JSON file
    if len(sys.argv) < 2:
        print("Usage: python generate_dgraph_script.py <path_to_input_json_file>")
        sys.exit(1)
        
    input_file = sys.argv[1]
    output_directory = "dgraph_output"
    
    generate_dgraph_schema_and_mutations(input_file, output_directory)