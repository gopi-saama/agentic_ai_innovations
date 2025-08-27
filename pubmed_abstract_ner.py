#!/usr/bin/env python3
import os
import sys
import json
from pathlib import Path
import pubmed_parser as pp
from tqdm import tqdm
import concurrent.futures
import threading
import transformers
import torch
import re

def extract_abstracts_from_file(file_path):
    """
    Extract abstracts from a single PubMed XML file
    
    Args:
        file_path (str): Path to the XML file (gzipped)
        
    Returns:
        dict: Dictionary of PMIDs and their associated abstracts
    """
    try:
        # Parse article data
        articles = list(pp.parse_medline_xml(file_path))
        
        # Extract articles with non-empty abstracts
        abstracts_dict = {}
        for article in articles:
            abstract = article.get('abstract', '').strip()
            if abstract:
                pmid = article.get('pmid', '')
                if pmid:
                    abstracts_dict[pmid] = {
                        'title': article.get('title', ''),
                        'abstract': abstract,
                        'journal': article.get('journal', ''),
                        'pubdate': article.get('pubdate', '')
                    }
        
        return abstracts_dict
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return {}

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

def perform_ner_on_abstract(abstract_text: str, title: str = None) -> dict:
    """
    Perform Named Entity Recognition on a PubMed abstract using the Llama3-Med42-70B model.
    
    Args:
        abstract_text (str): The abstract text to analyze
        title (str, optional): The title of the article for additional context
        
    Returns:
        dict: Dictionary with entity types as keys and lists of entities as values
    """
    # Initialize the model
    # model_name_or_path = "m42-health/Llama3-Med42-70B"
    model_name_or_path = "m42-health/Llama3-Med42-8B"
    
    # Create a pipeline for text generation
    try:
        pipeline = transformers.pipeline(
            "text-generation",
            model=model_name_or_path,
            torch_dtype=torch.bfloat16,
            device_map="auto",
        )
    except Exception as e:
        print(f"Error initializing model: {e}")
        return {"error": [str(e)]}
    
    # Construct the prompt with system and user messages
    context = title + ". " + abstract_text if title else abstract_text
    messages = [
        {
            "role": "system",
            "content": (
                "You are a medical entity extraction assistant specialized in biomedical text analysis. "
                "Extract named entities from the provided biomedical abstract into these categories: "
                "DISEASE (diseases and disorders), CHEMICAL (drugs, compounds), SPECIES (organisms), "
                "GENE (genes and proteins), and PROCEDURE (medical procedures). "
                "Return ONLY a JSON object with these categories as keys and arrays of unique extracted entities as values. "
                "Do not include any explanation or additional text."
            ),
        },
        {"role": "user", "content": f"Extract entities from this biomedical abstract: {context}"},
    ]
    
    # Apply the chat template
    prompt = pipeline.tokenizer.apply_chat_template(
        messages, tokenize=False, add_generation_prompt=False
    )
    
    # Define stop tokens
    stop_tokens = [
        pipeline.tokenizer.eos_token_id,
        pipeline.tokenizer.convert_tokens_to_ids("<|eot_id|>"),
    ]
    
    # Generate the response
    try:
        outputs = pipeline(
            prompt,
            max_new_tokens=512,
            eos_token_id=stop_tokens,
            do_sample=True,
            temperature=0.1,  # Lower temperature for more deterministic output
            top_k=50,
            top_p=0.9,
        )
        
        # Extract the generated text
        generated_text = outputs[0]["generated_text"][len(prompt):]
        
        # Extract JSON from the response
        json_match = re.search(r'(\{.*\})', generated_text, re.DOTALL)
        if json_match:
            try:
                # Parse the JSON string
                entity_data = json.loads(json_match.group(1))
                
                # Ensure all expected categories exist
                expected_categories = ["DISEASE", "CHEMICAL", "SPECIES", "GENE", "PROCEDURE"]
                for category in expected_categories:
                    if category not in entity_data:
                        entity_data[category] = []
                
                return entity_data
            except json.JSONDecodeError:
                return {
                    "error": ["Failed to parse JSON from model output"],
                    "raw_output": generated_text
                }
        else:
            return {
                "error": ["No JSON found in model output"],
                "raw_output": generated_text
            }
    except Exception as e:
        return {"error": [str(e)]}

def process_abstracts_with_ner(abstracts_data: dict, batch_size: int = 10, max_abstracts: int = None) -> dict:
    """
    Process a collection of PubMed abstracts with NER to extract named entities
    
    Args:
        abstracts_data (dict): Dictionary of PMIDs and their associated abstracts
                                         (as returned by extract_abstracts_from_file)
        batch_size (int): Number of abstracts to process in each batch (to manage memory)
        max_abstracts (int, optional): Maximum number of abstracts to process, useful for testing
        
    Returns:
        dict: Enhanced dictionary with NER results added to each abstract
    """
    # Initialize the model only once for all abstracts
    # model_name_or_path = "m42-health/Llama3-Med42-70B"
    model_name_or_path = "m42-health/Llama3-Med42-8B"
    print(f"Initializing NER model: {model_name_or_path}")
    
    try:
        pipeline = transformers.pipeline(
            "text-generation",
            model=model_name_or_path,
            torch_dtype=torch.bfloat16,
            device_map="auto",
        )
    except Exception as e:
        print(f"Error initializing model: {e}")
        return abstracts_data
        
    # Process abstracts with progress tracking
    enhanced_abstracts = {}
    pmids = list(abstracts_data.keys())[:max_abstracts] if max_abstracts else list(abstracts_data.keys())
    
    # Create batches
    batches = [pmids[i:i + batch_size] for i in range(0, len(pmids), batch_size)]
    
    with tqdm(total=len(pmids), desc="Processing abstracts with NER") as pbar:
        for batch in batches:
            for pmid in batch:
                abstract_data = abstracts_data[pmid]
                abstract_text = abstract_data.get('abstract', '')
                title = abstract_data.get('title', '')
                
                # Skip if no abstract
                if not abstract_text:
                    pbar.update(1)
                    continue
                
                # Reuse the common pipeline instance instead of creating a new one each time
                try:
                    # Create messages with the existing pipeline
                    context = title + ". " + abstract_text if title else abstract_text
                    messages = [
                        {
                            "role": "system",
                            "content": (
                                "You are a medical entity extraction assistant specialized in biomedical text analysis. "
                                "Extract named entities from the provided biomedical abstract into these categories: "
                                "DISEASE (diseases and disorders), CHEMICAL (drugs, compounds), SPECIES (organisms), "
                                "GENE (genes and proteins), and PROCEDURE (medical procedures). "
                                "Return ONLY a JSON object with these categories as keys and arrays of unique extracted entities as values. "
                                "Do not include any explanation or additional text."
                            ),
                        },
                        {"role": "user", "content": f"Extract entities from this biomedical abstract: {context}"},
                    ]
                    
                    # Apply the chat template
                    prompt = pipeline.tokenizer.apply_chat_template(
                        messages, tokenize=False, add_generation_prompt=False
                    )
                    
                    # Define stop tokens
                    stop_tokens = [
                        pipeline.tokenizer.eos_token_id,
                        pipeline.tokenizer.convert_tokens_to_ids("<|eot_id|>"),
                    ]
                    
                    # Generate the response
                    outputs = pipeline(
                        prompt,
                        max_new_tokens=512,
                        eos_token_id=stop_tokens,
                        do_sample=True,
                        temperature=0.1,  # Lower temperature for more deterministic output
                        top_k=50,
                        top_p=0.9,
                    )
                    
                    # Extract the generated text
                    generated_text = outputs[0]["generated_text"][len(prompt):]
                    
                    # Extract JSON from the response
                    json_match = re.search(r'(\{.*\})', generated_text, re.DOTALL)
                    if json_match:
                        # Parse the JSON string
                        entity_data = json.loads(json_match.group(1))
                        
                        # Ensure all expected categories exist
                        expected_categories = ["DISEASE", "CHEMICAL", "SPECIES", "GENE", "PROCEDURE"]
                        for category in expected_categories:
                            if category not in entity_data:
                                entity_data[category] = []
                                
                        # Add entities to the abstract data
                        abstract_data['entities'] = entity_data
                    else:
                        abstract_data['entities'] = {
                            "error": ["No JSON found in model output"],
                            "raw_output": generated_text[:200] + "..." if len(generated_text) > 200 else generated_text
                        }
                
                except Exception as e:
                    abstract_data['entities'] = {"error": [str(e)]}
                
                # Add to enhanced abstracts
                enhanced_abstracts[pmid] = abstract_data
                pbar.update(1)
    
    print(f"NER processing completed for {len(enhanced_abstracts)} abstracts")
    return enhanced_abstracts

def main():
    # Define the input directory
    input_directory = "/Users/gopinath.balu/Workspace/agentic_ai_innovations/pubmed_baseline2025"
    # Define output file
    output_dir = Path("/Users/gopinath.balu/Workspace/agentic_ai_innovations/intermediate_parsed/")
    output_dir.mkdir(exist_ok=True)
    abstracts_output = output_dir / "pubmed_abstracts.json"
    
    limit_files = 1
    # Find all xml.gz files in the input directory
    xml_files = list(Path(input_directory).glob('*.xml.gz'))[:limit_files]
    
    if not xml_files:
        print(f"No XML files found in {input_directory}")
        return
    
    print(f"Found {len(xml_files)} XML files to process")
    
    # Variables to track progress and collect abstracts
    all_abstracts = {}
    lock = threading.Lock()
    
    # Progress bar setup
    pbar = tqdm(
        total=len(xml_files),
        desc="Processing PubMed files",
        unit="file"
    )
    
    # Callback function to process results from each worker
    def process_result(future):
        try:
            file_abstracts = future.result()
            with lock:
                all_abstracts.update(file_abstracts)
                pbar.update(1)
                # Update description with current count
                pbar.set_description(f"Processing PubMed files (found {len(all_abstracts)} abstracts)")
        except Exception as e:
            print(f"Error processing a file: {e}")
            pbar.update(1)
    
    # Configure number of workers for concurrent processing
    max_workers = os.cpu_count() * 2
    
    # Use ThreadPoolExecutor for concurrent processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks to the executor
        futures = []
        for xml_file in xml_files:
            future = executor.submit(extract_abstracts_from_file, str(xml_file))
            future.add_done_callback(process_result)
            futures.append(future)
        
        # Wait for all tasks to complete
        for _ in concurrent.futures.as_completed(futures):
            pass
    
    # Close the progress bar
    pbar.close()
    
    # Print the final count and save to JSON
    print(f"\nTotal number of abstracts found: {len(all_abstracts)}")
    save_to_json(all_abstracts, abstracts_output)
    
    # Process abstracts with NER
    enhanced_abstracts = process_abstracts_with_ner(all_abstracts)
    ner_output = output_dir / "pubmed_abstracts_with_ner.json"
    save_to_json(enhanced_abstracts, ner_output)

if __name__ == "__main__":
    main()