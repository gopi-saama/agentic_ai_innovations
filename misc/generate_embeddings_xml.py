#!/usr/bin/env python3
# Memory-mapped progress tracking

import os
import gzip
import xml.etree.ElementTree as ET
import sys
from pathlib import Path
import logging
import mmap
import struct

from biomedbert_embedding import SentenceEmbedder

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("embedding_process.log"),
        logging.StreamHandler()
    ]
)

class MmapProgressTracker:
    """Progress tracker using memory-mapped files for efficient I/O."""
    
    # Format: Fixed-length fields for file name (255 bytes) and PMID (20 bytes)
    FORMAT = '255s20s'
    SIZE = struct.calcsize(FORMAT)
    
    def __init__(self, progress_file="progress.mmap"):
        self.progress_file = progress_file
        self.current_file = None
        self.current_pmid = None
        self.mm = None
        self.file_obj = None
        self._initialize_mmap()
        
    def _initialize_mmap(self):
        """Initialize or open the memory-mapped file."""
        exists = os.path.exists(self.progress_file)
        
        # Create file if it doesn't exist
        if not exists:
            with open(self.progress_file, "wb") as f:
                # Initialize with empty values (zeros)
                f.write(b'\0' * self.SIZE)
        
        # Open the file for reading and writing
        self.file_obj = open(self.progress_file, "r+b")
        
        # Create memory mapping
        self.mm = mmap.mmap(self.file_obj.fileno(), self.SIZE)
        
        # Load existing progress if file existed
        if exists:
            self.load()
    
    def load(self):
        """Load progress data from the memory-mapped file."""
        try:
            # Position at the beginning of the file
            self.mm.seek(0)
            
            # Unpack data according to format
            raw_file, raw_pmid = struct.unpack(self.FORMAT, self.mm.read(self.SIZE))
            
            # Convert bytes to strings and strip null bytes
            file_str = raw_file.decode('utf-8').rstrip('\0')
            pmid_str = raw_pmid.decode('utf-8').rstrip('\0')
            
            # Only set if values are not empty
            if file_str:
                self.current_file = file_str
                self.current_pmid = pmid_str
                logging.info(f"Loaded progress: file={self.current_file}, pmid={self.current_pmid}")
        except Exception as e:
            logging.error(f"Error loading progress from memory-mapped file: {str(e)}")
            self.current_file = None
            self.current_pmid = None
    
    def update(self, file_name, pmid):
        """Update progress by writing directly to memory-mapped region."""
        # Only update if there's a change
        if self.current_file != file_name or self.current_pmid != pmid:
            self.current_file = file_name
            self.current_pmid = pmid
            
            try:
                # Position at the beginning of the file
                self.mm.seek(0)
                
                # Pack data according to format
                file_bytes = file_name.encode('utf-8').ljust(255, b'\0')[:255]
                pmid_bytes = pmid.encode('utf-8').ljust(20, b'\0')[:20]
                
                # Write packed data to memory-mapped file
                self.mm.write(struct.pack(self.FORMAT, file_bytes, pmid_bytes))
                
                # Flush changes to disk to ensure persistence
                self.mm.flush()
                os.fsync(self.file_obj.fileno())
            except Exception as e:
                logging.error(f"Error updating memory-mapped progress file: {str(e)}")
    
    def clear(self):
        """Clear progress tracking."""
        self.current_file = None
        self.current_pmid = None
        
        try:
            # Write zeros to the entire file
            self.mm.seek(0)
            self.mm.write(b'\0' * self.SIZE)
            self.mm.flush()
            os.fsync(self.file_obj.fileno())
        except Exception as e:
            logging.error(f"Error clearing memory-mapped file: {str(e)}")
    
    def write(self):
        """
        Compatibility method with the previous API.
        For memory-mapped files, updates are immediate, so this is a no-op.
        """
        pass
    
    def __del__(self):
        """Clean up resources when object is destroyed."""
        if hasattr(self, 'mm') and self.mm:
            try:
                self.mm.close()
            except Exception:
                pass
                
        if hasattr(self, 'file_obj') and self.file_obj:
            try:
                self.file_obj.close()
            except Exception:
                pass

def extract_abstract_text(abstract_elem):
    """Extract text from abstract element, handling complex structures."""
    if abstract_elem is None:
        return None
        
    # If abstract has direct text
    if abstract_elem.text and abstract_elem.text.strip():
        text = abstract_elem.text.strip()
        # Also check for any paragraph elements
        paragraphs = abstract_elem.findall('.//p')
        if paragraphs:
            for p in paragraphs:
                if p.text and p.text.strip():
                    text += " " + p.text.strip()
        return text
    
    # If abstract has paragraph elements
    paragraphs = abstract_elem.findall('.//p')
    if paragraphs:
        text = " ".join([p.text.strip() for p in paragraphs if p.text and p.text.strip()])
        if text:
            return text
    
    return None

def process_xml_files(directory, embedder, progress_tracker):
    """Process all .gz XML files in the specified directory."""
    resume_mode = progress_tracker.current_file is not None
    
    # Get list of all .gz files in the directory
    gz_files = sorted([f for f in os.listdir(directory) if f.endswith('.gz')])
    
    # If resuming, skip to the current file
    if resume_mode and progress_tracker.current_file in gz_files:
        start_idx = gz_files.index(progress_tracker.current_file)
        gz_files = gz_files[start_idx:]
        logging.info(f"Resuming from file: {progress_tracker.current_file}, article PMID: {progress_tracker.current_pmid}")
    
    for file_name in gz_files:
        file_path = os.path.join(directory, file_name)
        logging.info(f"Processing file: {file_path}")
        
        # Process the XML file
        try:
            process_xml_file(file_path, embedder, progress_tracker)
        except Exception as e:
            logging.error(f"Error processing {file_path}: {str(e)}")
            # Progress tracker maintains last state for resume automatically
            
def process_xml_file(file_path, embedder, progress_tracker):
    """Process a single gzipped XML file."""
    resume_mode = progress_tracker.current_file == os.path.basename(file_path) and progress_tracker.current_pmid
    found_resume_point = not resume_mode  # If not resuming, we've already "found" it
    
    # Create a temporary file for writing the modified XML
    temp_output_path = file_path + '.temp'
    
    # Track processing stats
    processed_count = 0
    embedded_count = 0
    
    # Open the input gzipped file
    with gzip.open(file_path, 'rb') as f_in:
        # Create output gzipped file
        with gzip.open(temp_output_path, 'wb') as f_out:
            # Write XML declaration
            f_out.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
            f_out.write(b'<root>\n')
            
            # Parse XML incrementally
            context = ET.iterparse(f_in, events=('start', 'end'))
            
            for event, elem in context:
                if event == 'end' and elem.tag == 'article':
                    processed_count += 1
                    
                    # Extract article ID (PMID)
                    pmid = None
                    pmid_elem = elem.find('.//PMID')
                    if pmid_elem is not None and pmid_elem.text:
                        pmid = pmid_elem.text
                    
                    if pmid is None:
                        elem.clear()
                        continue
                    
                    # If resuming and haven't found the resume point yet
                    if resume_mode and not found_resume_point:
                        if pmid == progress_tracker.current_pmid:
                            found_resume_point = True
                            logging.info(f"Found resume point at PMID: {pmid}")
                        else:
                            # Write the element as-is and clear
                            f_out.write(ET.tostring(elem, encoding='utf-8'))
                            elem.clear()
                            continue
                    
                    # Process abstract
                    abstract_elem = elem.find('.//Abstract')
                    abstract_text = extract_abstract_text(abstract_elem)
                    
                    # Create a new element for the embedding - always add this tag
                    embedding_elem = ET.SubElement(elem, 'abstract_embeddings')
                    
                    if abstract_text:
                        try:
                            # Generate embedding for the abstract
                            embedding = embedder.encode(abstract_text)
                            
                            # Add embedding as text of the element
                            embedding_elem.text = str(embedding.tolist())
                            
                            # Update progress after successful embedding (now using memory-mapped file)
                            # Use basename to avoid storing full paths
                            progress_tracker.update(os.path.basename(file_path), pmid)
                            
                            embedded_count += 1
                            if embedded_count % 100 == 0:
                                logging.info(f"Processed {processed_count} articles, embedded {embedded_count} abstracts in {file_path}")
                                
                        except Exception as e:
                            logging.error(f"Error generating embedding for PMID {pmid}: {str(e)}")
                    # If no abstract text, embedding_elem will be an empty tag
                    
                    # Write the modified element
                    f_out.write(ET.tostring(elem, encoding='utf-8'))
                    
                    # Clear element to free memory
                    elem.clear()
            
            # Close the root element
            f_out.write(b'</root>')
    
    # Replace the original file with the temp file
    if os.path.exists(temp_output_path):
        os.remove(file_path)  # Remove original
        os.rename(temp_output_path, file_path)  # Rename temp to original
        logging.info(f"Completed processing {file_path}, processed {processed_count} articles, embedded {embedded_count} abstracts")

def main():
    if len(sys.argv) < 2:
        print("Usage: python generate_embeddings_xml.py <directory_with_gz_files>")
        sys.exit(1)
    
    directory = sys.argv[1]
    
    if not os.path.isdir(directory):
        logging.error(f"Directory does not exist: {directory}")
        sys.exit(1)
    
    # Initialize progress tracker with memory-mapped file
    progress_tracker = MmapProgressTracker()
    
    # Initialize the SentenceEmbedder
    try:
        embedder = SentenceEmbedder()
        logging.info("Successfully initialized SentenceEmbedder")
    except Exception as e:
        logging.error(f"Failed to initialize SentenceEmbedder: {str(e)}")
        sys.exit(1)
    
    try:
        # Process all XML files
        process_xml_files(directory, embedder, progress_tracker)
        
        # Clear progress when complete
        progress_tracker.clear()
        
        logging.info("Processing complete!")
    except KeyboardInterrupt:
        logging.info("Process interrupted. Progress saved automatically via memory-mapped file.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unhandled error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()