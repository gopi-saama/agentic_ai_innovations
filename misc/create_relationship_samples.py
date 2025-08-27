#!/usr/bin/env python3
import os
import csv
import glob

def create_relationship_markdown(relationship_dir, output_file):
    """
    Create a markdown file with samples from all CSV relationship files.
    
    Args:
        relationship_dir (str): Directory containing relationship CSV files
        output_file (str): Path to output markdown file
    """
    # Find all CSV files in the directory
    csv_files = sorted(glob.glob(os.path.join(relationship_dir, "*.csv")))
    
    with open(output_file, 'w') as md_file:
        # Write the markdown header
        md_file.write("# PubMed Knowledge Graph Relationship Samples\n\n")
        
        for i, csv_file in enumerate(csv_files):
            # Add separator if not the first file
            if i > 0:
                md_file.write("\n<!-- =============== RELATIONSHIP SEPARATOR =============== -->\n\n")
            
            # Get the file name for the header
            file_name = os.path.basename(csv_file)
            md_file.write(f"## {file_name}\n```\n")
            
            # Read and write 5 samples (header + 5 data rows)
            with open(csv_file, 'r', newline='') as f:
                csv_reader = csv.reader(f)
                for j, row in enumerate(csv_reader):
                    if j <= 5:  # Header + 5 samples
                        md_file.write(','.join(row) + '\n')
                    else:
                        break
            
            md_file.write("```\n")
    
    print(f"Markdown file created: {output_file}")

if __name__ == "__main__":
    # Paths
    relationship_dir = "/Users/gopinath.balu/Workspace/agentic_ai_innovations/constructed_KG/csv/relationships"
    output_file = "/Users/gopinath.balu/Workspace/agentic_ai_innovations/relationship_samples.md"
    
    # Create the markdown file
    create_relationship_markdown(relationship_dir, output_file)