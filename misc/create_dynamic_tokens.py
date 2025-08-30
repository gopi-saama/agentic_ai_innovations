import random
import torch
import sys
import os
import time
import gc
import psutil
from pathlib import Path
import numpy as np

# Add parent directory to path to import from sibling module
sys.path.append(str(Path(__file__).parent))

# Import the SentenceEmbedder from biomedbert_embedding.py
from biomedbert_embedding import SentenceEmbedder

def generate_random_sentence(word_count=10, include_biomedical_terms=True):
    """
    Generate a random sentence with approximately the specified number of words.
    Optionally includes biomedical terms to create more domain-specific test data.
    
    Args:
        word_count (int): Approximate number of words to include in the sentence.
        include_biomedical_terms (bool): Whether to include biomedical terms.
        
    Returns:
        str: A randomly generated sentence.
    """
    # Common English words for sentence construction
    common_words = [
        "the", "a", "an", "and", "but", "or", "because", "as", "with", "by",
        "for", "in", "to", "from", "at", "on", "is", "was", "were", "are",
        "have", "has", "had", "been", "will", "would", "could", "should",
        "patient", "doctor", "hospital", "treatment", "case", "study", "research",
    ]
    
    # Biomedical terms for domain-specific testing
    biomedical_terms = [
        "protein", "enzyme", "antibody", "antigen", "cell", "tissue", "gene",
        "hormone", "receptor", "virus", "bacteria", "infection", "disease",
        "diagnosis", "prognosis", "therapy", "treatment", "clinical", "pathology",
        "oncology", "cardiology", "neurology", "immunology", "medication", "dosage",
        "symptom", "syndrome", "chronic", "acute", "remission"
    ]
    
    # Verbs to make sentences more natural
    verbs = [
        "affects", "inhibits", "activates", "regulates", "binds", "blocks",
        "stimulates", "suppresses", "treats", "causes", "prevents", "reduces",
        "increases", "decreases", "modulates", "interacts", "targets"
    ]
    
    # Adjectives for more variety
    adjectives = [
        "significant", "severe", "mild", "chronic", "acute", "clinical",
        "experimental", "therapeutic", "diagnostic", "pathogenic", "novel",
        "effective", "potential", "promising", "adverse", "beneficial"
    ]
    
    # Build word pool based on parameters
    word_pool = common_words.copy()
    if include_biomedical_terms:
        word_pool.extend(biomedical_terms)
        word_pool.extend(verbs)
        word_pool.extend(adjectives)
    
    # Generate a random sentence structure with subject-verb-object pattern
    sentence_words = []
    
    # Start with a determiner and possibly an adjective
    sentence_words.append(random.choice(["The", "A", "This", "Our"]))
    if random.random() > 0.5:
        sentence_words.append(random.choice(adjectives))
    
    # Add a subject (noun)
    subject_options = biomedical_terms if include_biomedical_terms else common_words
    sentence_words.append(random.choice(subject_options))
    
    # Add a verb
    sentence_words.append(random.choice(verbs))
    
    # Add an object with possible determiner and adjective
    sentence_words.append(random.choice(["the", "a", "this"]))
    if random.random() > 0.5:
        sentence_words.append(random.choice(adjectives))
    
    # Add an object (noun)
    object_options = biomedical_terms if include_biomedical_terms else common_words
    sentence_words.append(random.choice(object_options))
    
    # Continue adding words until we reach the desired count
    while len(sentence_words) < word_count:
        # Add conjunctions or prepositions
        if random.random() > 0.7:
            sentence_words.append(random.choice(["and", "or", "but", "with", "in", "through"]))
        
        # Add more noun phrases
        if random.random() > 0.5:
            sentence_words.append(random.choice(["the", "a", "this"]))
        if random.random() > 0.5:
            sentence_words.append(random.choice(adjectives))
        
        # Add another noun
        term_options = biomedical_terms if include_biomedical_terms and random.random() > 0.3 else common_words
        sentence_words.append(random.choice(term_options))
    
    # Trim to desired length
    sentence_words = sentence_words[:word_count]
    
    # Ensure the sentence ends with a period
    sentence = " ".join(sentence_words) + "."
    
    # Capitalize the first letter
    return sentence[0].upper() + sentence[1:]

def generate_sentence_batch(batch_size=5, min_words=8, max_words=20000):
    """
    Generate a batch of random sentences with varying lengths.
    
    Args:
        batch_size (int): Number of sentences to generate
        min_words (int): Minimum words per sentence
        max_words (int): Maximum words per sentence
        
    Returns:
        list: A list of randomly generated sentences
    """
    sentences = []
    for _ in range(batch_size):
        word_count = random.randint(min_words, max_words)
        sentences.append(generate_random_sentence(word_count=word_count))
    
    return sentences

def test_embeddings_with_random_sentences(sentences, embedder=None):
    """
    Tests the SentenceEmbedder with a list of random sentences.
    
    Args:
        sentences (list): List of sentences to embed
        embedder (SentenceEmbedder, optional): An existing embedder instance or None to create new
        
    Returns:
        tuple: (embeddings, similarity_matrix) containing the embeddings and pairwise similarities
    """
    # Initialize the embedder if not provided
    if embedder is None:
        print("Initializing BiomedBERT embedder...")
        embedder = SentenceEmbedder()
        
    # Generate embeddings for all sentences
    print(f"Generating embeddings for {len(sentences)} sentences...")
    embeddings = embedder.encode(sentences)
    
    # Calculate pairwise similarities
    similarity_matrix = np.zeros((len(sentences), len(sentences)))
    
    print("Calculating similarity matrix...")
    for i in range(len(sentences)):
        for j in range(i, len(sentences)):  # Only calculate upper triangle
            sim = embedder.get_similarity(sentences[i], sentences[j])
            similarity_matrix[i, j] = sim
            if i != j:  # Mirror the matrix for lower triangle
                similarity_matrix[j, i] = sim
    
    return embeddings, similarity_matrix

def display_results(sentences, similarity_matrix):
    """
    Display the results of sentence similarity analysis.
    
    Args:
        sentences (list): List of sentences
        similarity_matrix (numpy.ndarray): Matrix of pairwise similarities
    """
    print("\n===== SENTENCE SIMILARITY RESULTS =====")
    
    # Find most and least similar pairs
    max_sim = -1
    min_sim = 2  # Cosine similarity is between -1 and 1
    max_pair = (0, 0)
    min_pair = (0, 0)
    
    for i in range(len(sentences)):
        for j in range(i+1, len(sentences)):  # Skip diagonal and lower triangle
            sim = similarity_matrix[i, j]
            if sim > max_sim:
                max_sim = sim
                max_pair = (i, j)
            if sim < min_sim:
                min_sim = sim
                min_pair = (i, j)
    
    # Print results
    print(f"\nMost similar pair (similarity = {max_sim:.4f}):")
    print(f"1: \"{sentences[max_pair[0]]}\"")
    print(f"2: \"{sentences[max_pair[1]]}\"")
    
    print(f"\nLeast similar pair (similarity = {min_sim:.4f}):")
    print(f"1: \"{sentences[min_pair[0]]}\"")
    print(f"2: \"{sentences[min_pair[1]]}\"")
    
    # Print full similarity matrix if not too large
    if len(sentences) <= 5:
        print("\nFull similarity matrix:")
        # Print header
        print("    " + "".join([f"{i:^10}" for i in range(len(sentences))]))
        for i in range(len(sentences)):
            row = [f"{i:2}:"] + [f"{similarity_matrix[i, j]:.4f}".center(10) for j in range(len(sentences))]
            print(" ".join(row))

def test_max_batch_size(min_batch=1, max_batch=1000, step_size=10, word_count=20, timeout_seconds=300):
    """
    Test increasingly large batch sizes to find the maximum batch size that can be processed
    without running out of memory or timing out.
    
    Args:
        min_batch (int): Starting batch size
        max_batch (int): Maximum batch size to try
        step_size (int): Increment size for testing batches
        word_count (int): Number of words per sentence
        timeout_seconds (int): Maximum time allowed for a single batch test in seconds
        
    Returns:
        dict: Results of testing with information on successful and failed batch sizes
    """
    print("\n=== TESTING MAXIMUM BATCH SIZE ===")
    
    results = {
        "max_successful_batch": 0,
        "min_failed_batch": None,
        "batch_results": [],
        "failure_reason": None
    }
    
    # Initialize the embedder
    print("Initializing BiomedBERT embedder...")
    embedder = SentenceEmbedder()
    
    process = psutil.Process(os.getpid())
    
    current_batch_size = min_batch
    while current_batch_size <= max_batch:
        batch_result = {
            "batch_size": current_batch_size,
            "success": False,
            "memory_before_mb": round(process.memory_info().rss / 1024 / 1024, 2),
            "memory_after_mb": None,
            "memory_increase_mb": None,
            "processing_time_sec": None,
            "error": None
        }
        
        print(f"\nTesting batch size: {current_batch_size}")
        print(f"Memory usage before: {batch_result['memory_before_mb']} MB")
        
        # Generate batch of sentences
        try:
            # Force garbage collection before generating new batch
            gc.collect()
            torch.cuda.empty_cache() if torch.cuda.is_available() else None
            
            # Generate sentences
            print(f"Generating {current_batch_size} sentences...")
            sentences = generate_sentence_batch(batch_size=current_batch_size, 
                                                min_words=word_count, 
                                                max_words=word_count)
            
            # Time the embedding process
            start_time = time.time()
            
            # Set a timeout using a separate thread
            timed_out = [False]
            
            def timeout_handler():
                time.sleep(timeout_seconds)
                if time.time() - start_time >= timeout_seconds:
                    timed_out[0] = True
            
            # Start timeout monitor in separate thread
            import threading
            timer = threading.Thread(target=timeout_handler)
            timer.daemon = True
            timer.start()
            
            # Generate embeddings
            print(f"Generating embeddings for batch size {current_batch_size}...")
            embeddings = embedder.encode(sentences)
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            if timed_out[0]:
                raise TimeoutError(f"Processing took longer than {timeout_seconds} seconds")
            
            # Record memory usage after processing
            batch_result["memory_after_mb"] = round(process.memory_info().rss / 1024 / 1024, 2)
            batch_result["memory_increase_mb"] = batch_result["memory_after_mb"] - batch_result["memory_before_mb"]
            batch_result["processing_time_sec"] = round(processing_time, 2)
            batch_result["success"] = True
            
            print(f"Successfully processed batch size: {current_batch_size}")
            print(f"Memory usage after: {batch_result['memory_after_mb']} MB")
            print(f"Memory increase: {batch_result['memory_increase_mb']} MB")
            print(f"Processing time: {batch_result['processing_time_sec']} seconds")
            
            # Update the maximum successful batch size
            results["max_successful_batch"] = current_batch_size
            
        except (RuntimeError, TimeoutError, Exception) as e:
            error_message = str(e)
            batch_result["error"] = error_message
            print(f"Failed at batch size {current_batch_size}: {error_message}")
            
            if results["min_failed_batch"] is None:
                results["min_failed_batch"] = current_batch_size
                results["failure_reason"] = error_message
            
            # If we've already had a successful batch and now hit a failure,
            # try to find a more precise maximum with smaller step size
            if results["max_successful_batch"] > 0:
                # If the step size is already 1, we've found the exact maximum
                if step_size == 1:
                    break
                
                # Otherwise, go back to the last successful batch and use a smaller step
                current_batch_size = results["max_successful_batch"]
                step_size = max(1, step_size // 2)
                print(f"Refining search with smaller step size: {step_size}")
                current_batch_size += step_size
                continue
        
        # Record the result
        results["batch_results"].append(batch_result)
        
        # Increase batch size for next iteration
        current_batch_size += step_size
    
    # Print final results
    print("\n=== BATCH SIZE TEST RESULTS ===")
    print(f"Maximum successful batch size: {results['max_successful_batch']}")
    if results["min_failed_batch"]:
        print(f"Minimum failed batch size: {results['min_failed_batch']}")
        print(f"Failure reason: {results['failure_reason']}")
    
    return results

if __name__ == "__main__":
    print("=== Testing BiomedBERT Embeddings with Maximum Batch Size ===\n")
    
    # Test with different batch sizes
    batch_test_results = test_max_batch_size(
        min_batch=1,
        max_batch=200,  # Start with a reasonable maximum to test
        step_size=10,   # Increment by 10 initially
        word_count=20,  # Keep sentences relatively short for testing
        timeout_seconds=300  # 5-minute timeout per batch test
    )
    
    # Print detailed results
    print("\n=== DETAILED TEST RESULTS ===")
    for result in batch_test_results["batch_results"]:
        status = "SUCCESS" if result["success"] else "FAILED"
        print(f"Batch size {result['batch_size']}: {status}")
        if result["success"]:
            print(f"  Memory before: {result['memory_before_mb']} MB")
            print(f"  Memory after: {result['memory_after_mb']} MB")
            print(f"  Memory increase: {result['memory_increase_mb']} MB")
            print(f"  Processing time: {result['processing_time_sec']} seconds")
        else:
            print(f"  Error: {result['error']}")
    
    print(f"\nRecommended maximum batch size: {batch_test_results['max_successful_batch']}")
    
    # Optional: Test embedding quality with the maximum batch size
    if batch_test_results["max_successful_batch"] > 0:
        print("\n=== Testing embedding quality with maximum batch size ===")
        max_batch = batch_test_results["max_successful_batch"]
        sentences = generate_sentence_batch(batch_size=max_batch, min_words=20, max_words=20)
        
        embedder = SentenceEmbedder()
        embeddings = embedder.encode(sentences)
        
        print(f"Generated embeddings shape: {embeddings.shape}")
        
        # Test similarities between first 5 sentences only (to avoid large output)
        test_sample = sentences[:min(5, len(sentences))]
        _, sim_matrix = test_embeddings_with_random_sentences(test_sample, embedder)
        display_results(test_sample, sim_matrix)