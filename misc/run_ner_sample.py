#!/usr/bin/env python3
import json
import sys
from pathlib import Path
from pubmed_abstract_ner import perform_ner_on_abstract

def main():
    # Path to the JSON file with abstracts
    json_path = Path("/Users/gopinath.balu/Workspace/agentic_ai_innovations/intermediate_parsed/pubmed_abstracts.json")
    
    # Load the abstracts
    print(f"Loading abstracts from {json_path}...")
    with open(json_path, 'r', encoding='utf-8') as f:
        abstracts_data = json.load(f)
    
    # Display available PMIDs
    print(f"Loaded {len(abstracts_data)} abstracts.")
    print("Available PubMed IDs (first 5):")
    for i, pmid in enumerate(list(abstracts_data.keys())[:5]):
        title = abstracts_data[pmid]['title']
        print(f"{i+1}. PMID: {pmid} - Title: {title[:50]}{'...' if len(title) > 50 else ''}")
    
    # Ask user to select a PMID
    selected_pmid = input("\nEnter a PubMed ID from the list above: ").strip()
    
    # Check if the selected PMID exists
    if selected_pmid in abstracts_data:
        # Get the abstract and title
        abstract_text = abstracts_data[selected_pmid]['abstract']
        title = abstracts_data[selected_pmid]['title']
        
        print(f"\nSelected abstract (PMID: {selected_pmid}):")
        print(f"Title: {title}")
        print(f"Abstract: {abstract_text[:100]}...\n")
        
        # Perform NER on the selected abstract
        print("Performing NER analysis...")
        results = perform_ner_on_abstract(abstract_text, title)
        
        # Display the results
        print("\nNER Results:")
        for entity_type, entities in results.items():
            if entity_type != "error" and entity_type != "raw_output":
                print(f"\n{entity_type}:")
                for entity in entities:
                    print(f"- {entity}")
            elif entity_type == "error":
                print(f"\nError occurred: {entities}")
    else:
        print(f"PubMed ID '{selected_pmid}' not found in the data.")

if __name__ == "__main__":
    main()